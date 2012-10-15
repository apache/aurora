package com.twitter.mesos.scheduler.log.mesos;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Provider;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Longs;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import org.apache.mesos.Log;

import com.twitter.common.base.Function;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A {@code Log} implementation backed by a true distributed log in mesos core.
 */
public class MesosLog implements com.twitter.mesos.scheduler.log.Log {

  private static final Logger LOG = Logger.getLogger(MesosLog.class.getName());

  /**
   * Binding annotation for the opaque value of a log noop entry.
   */
  @BindingAnnotation
  @Retention(RUNTIME)
  @Target({ PARAMETER, METHOD })
  public @interface NoopEntry { }

  /**
   * Binding annotation for log read timeouts.
   */
  @BindingAnnotation
  @Retention(RUNTIME)
  @Target({ PARAMETER, METHOD })
  public @interface ReadTimeout { }

  /**
   * Binding annotation for log write timeouts - used for truncates and appends.
   */
  @BindingAnnotation
  @Retention(RUNTIME)
  @Target({ PARAMETER, METHOD })
  public @interface WriteTimeout { }

  private final Provider<Log> logFactory;

  private final Provider<Log.Reader> readerFactory;
  private final Amount<Long, Time> readTimeout;

  private final Provider<Log.Writer> writerFactory;
  private final Amount<Long, Time> writeTimeout;

  private final byte[] noopEntry;

  /**
   * Creates a new mesos log.
   *
   * @param logFactory Factory to provide access to log.
   * @param readerFactory Factory to provide access to log readers.
   * @param readTimeout Log read timeout.
   * @param writerFactory Factory to provide access to log writers.
   * @param writeTimeout Log write timeout.
   * @param noopEntry A no-op log entry blob.
   */
  @Inject
  public MesosLog(
      Provider<Log> logFactory,
      Provider<Log.Reader> readerFactory,
      @ReadTimeout Amount<Long, Time> readTimeout,
      Provider<Log.Writer> writerFactory,
      @WriteTimeout Amount<Long, Time> writeTimeout,
      @NoopEntry byte[] noopEntry) {

    this.logFactory = Preconditions.checkNotNull(logFactory);

    this.readerFactory = Preconditions.checkNotNull(readerFactory);
    this.readTimeout = readTimeout;

    this.writerFactory = Preconditions.checkNotNull(writerFactory);
    this.writeTimeout = writeTimeout;

    this.noopEntry = Preconditions.checkNotNull(noopEntry);
  }

  @Override
  public Stream open() {
    return new LogStream(
        logFactory.get(), readerFactory.get(), readTimeout, writerFactory, writeTimeout, noopEntry);
  }

  private static class LogStream implements com.twitter.mesos.scheduler.log.Log.Stream {
    private static final class OpStats {
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

      private static AtomicLong exportLongStat(String template, Object... args) {
        return Stats.exportLong(String.format(template, args));
      }
    }

    private static final Function<Log.Entry, LogEntry> MESOS_ENTRY_TO_ENTRY =
        new Function<Log.Entry, LogEntry>() {
          @Override public LogEntry apply(Log.Entry entry) {
            return new LogEntry(entry);
          }
        };

    private final OpStats read = new OpStats("read");
    private final OpStats append = new OpStats("append");
    private final OpStats truncate = new OpStats("truncate");
    private final AtomicLong entriesSkipped =
        Stats.exportLong("scheduler_log_native_native_entries_skipped");

    private final Log log;

    private final Log.Reader reader;
    private final long readTimeout;
    private final TimeUnit readTimeUnit;

    private final Provider<Log.Writer> writerFactory;
    private final long writeTimeout;
    private final TimeUnit writeTimeUnit;

    private final byte[] noopEntry;

    private Log.Writer writer;

    LogStream(Log log, Log.Reader reader, Amount<Long, Time> readTimeout,
        Provider<Log.Writer> writerFactory, Amount<Long, Time> writeTimeout,
        byte[] noopEntry) {

      this.log = log;

      this.reader = reader;
      this.readTimeout = readTimeout.getValue();
      this.readTimeUnit = readTimeout.getUnit().getTimeUnit();

      this.writerFactory = writerFactory;
      this.writeTimeout = writeTimeout.getValue();
      this.writeTimeUnit = writeTimeout.getUnit().getTimeUnit();

      this.noopEntry = noopEntry;
    }

    @Timed("scheduler_log_native_read_from")
    @Override
    public Iterator<Entry> readAll() throws StreamAccessException {

      // TODO(John Sirois): Currently we must be the coordinator to ensure we get the 'full read'
      // of log entries expected by the users of the com.twitter.mesos.scheduler.log.Log interface.
      // Switch to another method of ensuring this when it becomes available in mesos' log
      // interface.
      try {
        append(noopEntry);
      } catch (StreamAccessException e) {
        throw new StreamAccessException("Error writing noop prior to a read", e);
      }

      final Log.Position from = reader.beginning();
      final Log.Position to = end().unwrap();

      // Reading all the entries at once may cause large garbage collections. Instead, we
      // lazily read the entries one by one as they are requested.
      // TODO(Benjamin Hindman): Eventually replace this functionality with functionality
      // from the Mesos Log.
      return new UnmodifiableIterator<Entry>() {
        private long position = Longs.fromByteArray(from.identity());
        private final long endPosition = Longs.fromByteArray(to.identity());
        private Entry entry = null;

        @Override
        public boolean hasNext() {
          if (entry != null) {
            return true;
          }

          while (position <= endPosition) {
            try {
              Log.Position p = log.position(Longs.toByteArray(position));
              LOG.info("Reading position " + position + " from the log");
              List<Log.Entry> entries = reader.read(p, p, readTimeout, readTimeUnit);

              // N.B. HACK! There is currently no way to "increment" a position. Until the Mesos
              // Log actually provides a way to "stream" the log, we approximate as much by
              // using longs via Log.Position.identity and Log.position.
              position++;

              // Reading positions in this way means it's possible that we get an "invalid" entry
              // (e.g., in the underlying log terminology this would be anything but an append)
              // which will be removed from the returned entries resulting in an empty list.
              // We skip these.
              if (entries.isEmpty()) {
                entriesSkipped.getAndIncrement();
                continue;
              } else {
                entry = MESOS_ENTRY_TO_ENTRY.apply(Iterables.getOnlyElement(entries));
                return true;
              }
            } catch (TimeoutException e) {
              read.timeouts.getAndIncrement();
              throw new StreamAccessException("Timeout reading from log.", e);
            } catch (Log.OperationFailedException e) {
              read.failures.getAndIncrement();
              throw new StreamAccessException("Problem reading from log", e);
            } finally {
              read.total.getAndIncrement();
            }
          }
          return false;
        }

        @Override
        public Entry next() {
          if (entry == null && !hasNext()) {
            throw new NoSuchElementException();
          }

          Entry result = Preconditions.checkNotNull(entry);
          entry = null;
          return result;
        }
      };
    }

    @Timed("scheduler_log_native_append")
    @Override
    public LogPosition append(final byte[] contents) throws StreamAccessException {
      Preconditions.checkNotNull(contents);

      Log.Position position = mutate(append, new Mutation<Log.Position>() {
        @Override public Log.Position apply(Log.Writer logWriter)
            throws TimeoutException, Log.WriterFailedException {
          return logWriter.append(contents, writeTimeout, writeTimeUnit);
        }
      });
      return LogPosition.wrap(position);
    }

    @Timed("scheduler_log_native_truncate_before")
    @Override
    public void truncateBefore(com.twitter.mesos.scheduler.log.Log.Position position)
        throws StreamAccessException {

      Preconditions.checkArgument(position instanceof LogPosition);

      final Log.Position before = ((LogPosition) position).unwrap();
      mutate(truncate, new Mutation<Void>() {
        @Override public Void apply(Log.Writer logWriter)
            throws TimeoutException, Log.WriterFailedException {
          logWriter.truncate(before, writeTimeout, writeTimeUnit);
          return null;
        }
      });
    }

    private interface Mutation<T> {
      T apply(Log.Writer writer) throws TimeoutException, Log.WriterFailedException;
    }

    private synchronized <T> T mutate(OpStats stats, Mutation<T> mutation) {
      if (writer == null) {
        writer = writerFactory.get();
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

    private LogPosition end() {
      return LogPosition.wrap(reader.ending());
    }

    @Override
    public void close() {
      // noop
    }

    private static class LogPosition implements com.twitter.mesos.scheduler.log.Log.Position {
      private final Log.Position underlying;

      LogPosition(Log.Position underlying) {
        this.underlying = underlying;
      }

      static LogPosition wrap(Log.Position position) {
        return new LogPosition(position);
      }

      Log.Position unwrap() {
        return underlying;
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
      public byte[] contents() {
        return underlying.data;
      }
    }
  }
}
