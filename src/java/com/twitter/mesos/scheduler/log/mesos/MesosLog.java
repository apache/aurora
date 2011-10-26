package com.twitter.mesos.scheduler.log.mesos;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import org.apache.mesos.Log;

import com.twitter.common.base.Function;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.stats.Stats;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A {@code Log} implementation backed by a true distributed log in mesos core.
 *
 * @author John Sirois
 */
public class MesosLog implements com.twitter.mesos.scheduler.log.Log {

  /**
   * Binding annotation for the opaque value of a log noop entry.
   */
  @BindingAnnotation
  @Retention(RUNTIME)
  @Target({PARAMETER, METHOD})
  public @interface NoopEntry { }

  private final Log log;
  private final byte[] noopEntry;

  @Inject
  public MesosLog(Log log, @NoopEntry byte[] noopEntry) {
    this.log = Preconditions.checkNotNull(log);
    this.noopEntry = Preconditions.checkNotNull(noopEntry);
  }

  @Override
  public Stream open() {
    return new LogStream(log, noopEntry);
  }

  private static class LogStream implements com.twitter.mesos.scheduler.log.Log.Stream {
    private static class OpStats {
      private static AtomicLong exportLongStat(String template, Object... args) {
        return Stats.exportLong(String.format(template, args));
      }

      final String opName;
      final AtomicLong total;
      final AtomicLong timeouts;
      final AtomicLong failures;

      private OpStats(String opName) {
        this.opName = MorePreconditions.checkNotBlank(opName);
        total = exportLongStat("scheduler_log_native_%s_total", opName);
        timeouts = exportLongStat("scheduler_log_native_%s_timeouts", opName);
        failures = exportLongStat("scheduler_log_native_%s_failures", opName);
      }
    }

    private static class Vars {
      final OpStats read = new OpStats("read");
      final OpStats append = new OpStats("append");
      final OpStats truncate = new OpStats("truncate");
    }
    private final Vars vars = new Vars();

    private final Log log;
    private final byte[] noopEntry;
    private final Log.Reader reader;

    LogStream(Log log, byte[] noopEntry) {
      this.log = log;
      this.noopEntry = noopEntry;
      this.reader = new Log.Reader(log);
    }

    private static final Function<Log.Entry, LogEntry> MESOS_ENTRY_TO_ENTRY =
        new Function<Log.Entry, LogEntry>() {
          @Override public LogEntry apply(Log.Entry entry) {
            return new LogEntry(entry);
          }
        };

    @Override
    public Iterator<Entry> readFrom(
        com.twitter.mesos.scheduler.log.Log.Position position) throws StreamAccessException {

      Preconditions.checkArgument(position instanceof LogPosition);

      // TODO(John Sirois): Currently we must be the coordinator to ensure we get the 'full read'
      // of log entries expected by the users of the com.twitter.mesos.scheduler.log.Log interface.
      // Switch to another method of ensuring this when it becomes available in mesos' log
      // interface.
      try {
        append(noopEntry);
      } catch (StreamAccessException e) {
        throw new StreamAccessException("Error writing noop prior to a read", e);
      }

      Log.Position from = ((LogPosition) position).unwrap();
      Log.Position to = end().unwrap();
      try {
        List<Log.Entry> entries = reader.read(from, to);
        return Iterables.<Log.Entry, Entry>transform(entries, MESOS_ENTRY_TO_ENTRY).iterator();
      } catch (TimeoutException e) {
        vars.read.timeouts.getAndIncrement();
        throw new StreamAccessException("Timeout reading from log.", e);
      } catch (Log.OperationFailedException e) {
        vars.read.failures.getAndIncrement();
        throw new StreamAccessException("Problem reading from log", e);
      } finally {
        vars.read.total.getAndIncrement();
      }
    }

    @Override
    public LogPosition append(final byte[] contents) throws StreamAccessException {
      Preconditions.checkNotNull(contents);

      Log.Position position = mutate(vars.append, new Mutation<Log.Position>() {
        @Override public Log.Position apply(Log.Writer writer)
            throws TimeoutException, Log.WriterFailedException {
          return writer.append(contents);
        }
      });
      return LogPosition.wrap(position);
    }

    @Override
    public void truncateBefore(com.twitter.mesos.scheduler.log.Log.Position position)
        throws StreamAccessException {

      Preconditions.checkArgument(position instanceof LogPosition);

      final Log.Position before = ((LogPosition) position).unwrap();
      mutate(vars.truncate, new Mutation<Void>() {
        @Override public Void apply(Log.Writer writer)
            throws TimeoutException, Log.WriterFailedException {
          writer.truncate(before);
          return null;
        }
      });
    }

    private interface Mutation<T> {
      T apply(Log.Writer writer) throws TimeoutException, Log.WriterFailedException;
    }

    private Log.Writer writer;

    private synchronized <T> T mutate(OpStats stats, Mutation<T> mutation) {
      if (writer == null) {
        writer = new Log.Writer(log);
      }
      try {
        return mutation.apply(writer);
      } catch (TimeoutException e) {
        stats.timeouts.getAndIncrement();
        throw new StreamAccessException("Timeout performing log " + stats.opName, e);
      } catch (Log.WriterFailedException e) {
        stats.failures.getAndIncrement();

        // We must throw away a writer on any write failure - this could be because of a coordinator
        // election in which case we must trigger a new election.
        writer = null;

        throw new StreamAccessException("Problem performing log" + stats.opName, e);
      } finally {
        stats.total.getAndIncrement();
      }
    }

    @Override
    public LogPosition beginning() {
      return LogPosition.wrap(reader.beginning());
    }

    @Override
    public LogPosition end() {
      return LogPosition.wrap(reader.ending());
    }

    @Override
    public LogPosition position(byte[] identity) {
      Preconditions.checkNotNull(identity);

      return LogPosition.wrap(log.position(identity));
    }

    @Override
    public void close() {
      // noop
    }

    private static class LogPosition implements com.twitter.mesos.scheduler.log.Log.Position {
      static LogPosition wrap(Log.Position position) {
        return new LogPosition(position);
      }

      private final Log.Position underlying;

      LogPosition(Log.Position underlying) {
        this.underlying = underlying;
      }

      Log.Position unwrap() {
        return underlying;
      }

      @Override public byte[] identity() {
        return underlying.identity();
      }

      @Override public int compareTo(Position o) {
        Preconditions.checkArgument(o instanceof LogPosition);
        return underlying.compareTo(((LogPosition) o).underlying);
      }
    }

    private static class LogEntry implements com.twitter.mesos.scheduler.log.Log.Entry {
      private final Log.Entry underlying;

      public LogEntry(Log.Entry entry) {
        this.underlying = entry;
      }

      @Override
      public LogPosition position() {
        return LogPosition.wrap(underlying.position);
      }

      @Override
      public byte[] contents() {
        return underlying.data;
      }
    }
  }
}
