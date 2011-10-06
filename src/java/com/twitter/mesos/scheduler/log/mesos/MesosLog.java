package com.twitter.mesos.scheduler.log.mesos;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.mesos.Log;

import com.twitter.common.base.Function;
import com.twitter.common.stats.Stats;

/**
 * A {@code Log} implementation backed by a true distributed log in mesos core.
 *
 * @author John Sirois
 */
public class MesosLog implements com.twitter.mesos.scheduler.log.Log {
  private final Log log;

  @Inject
  public MesosLog(Log log) {
    this.log = Preconditions.checkNotNull(log);
  }

  @Override
  public Stream open() {
    return new LogStream(log);
  }

  private static class LogStream implements com.twitter.mesos.scheduler.log.Log.Stream {
    private static class Vars {
      final AtomicLong readTimeouts = Stats.exportLong("scheduler_log_native_read_timeouts");
      final AtomicLong readFailures = Stats.exportLong("scheduler_log_native_read_failures");
      final AtomicLong appendTimeouts = Stats.exportLong("scheduler_log_native_append_timeouts");
      final AtomicLong appendFailures = Stats.exportLong("scheduler_log_native_append_failures");
      final AtomicLong truncateTimeouts =
          Stats.exportLong("scheduler_log_native_truncate_timeouts");
      final AtomicLong truncateFailures =
          Stats.exportLong("scheduler_log_native_truncate_failures");
      final AtomicLong invalidatedWriters =
          Stats.exportLong("scheduler_log_native_invalidated_writers");
    }
    private final Vars vars = new Vars();

    private final Log log;
    private final Log.Reader reader;
    private Log.Writer writer;

    LogStream(Log log) {
      this.log = log;
      this.reader = new Log.Reader(log);
    }

    private static final Function<Log.Entry, LogEntry> MESOS_ENTRY_TO_ENTRY =
        new Function<Log.Entry, LogEntry>() {
          @Override public LogEntry apply(Log.Entry entry) {
            return new LogEntry(entry);
          }
        };

    @Override
    public synchronized Iterator<Entry> readFrom(
        com.twitter.mesos.scheduler.log.Log.Position position) throws StreamAccessException {

      Preconditions.checkArgument(position instanceof LogPosition);

      Log.Position from = ((LogPosition) position).unwrap();
      Log.Position to = end().unwrap();
      try {
        List<Log.Entry> entries = reader.read(from, to);
        return Iterables.<Log.Entry, Entry>transform(entries, MESOS_ENTRY_TO_ENTRY).iterator();
      } catch (TimeoutException e) {
        vars.readTimeouts.getAndIncrement();
        throw new StreamAccessException("Timeout reading from log.", e);
      } catch (Log.OperationFailedException e) {
        vars.readFailures.getAndIncrement();
        throw new StreamAccessException("Problem reading from log", e);
      }
    }

    @Override
    public synchronized LogPosition append(byte[] contents)
        throws StreamAccessException {

      try {
        Log.Position position = writer().append(contents);
        return LogPosition.wrap(position);
      } catch (TimeoutException e) {
        vars.appendTimeouts.getAndIncrement();
        throw new StreamAccessException("Timeout appending entry to the log.", e);
      } catch (Log.WriterFailedException e) {
        vars.appendFailures.getAndIncrement();

        // We must throw away a writer on a write failure - this could be because of a coordinator
        // election in which case we must trigger a new election.
        invalidateWriter();

        throw new StreamAccessException("Problem appending entry to the log.", e);
      }
    }

    @Override
    public synchronized void truncateBefore(com.twitter.mesos.scheduler.log.Log.Position position)
        throws StreamAccessException {

      Preconditions.checkArgument(position instanceof LogPosition);

      Log.Position before = ((LogPosition) position).unwrap();
      try {
        writer().truncate(before);
      } catch (TimeoutException e) {
        vars.truncateTimeouts.getAndIncrement();
        throw new StreamAccessException("Timeout truncating log.", e);
      } catch (Log.WriterFailedException e) {
        vars.truncateFailures.getAndIncrement();

        // We must throw away a writer on a write failure - this could be because of a coordinator
        // election in which case we must trigger a new election.
        invalidateWriter();

        throw new StreamAccessException("Problem truncating log.", e);
      }
    }

    @Override
    public synchronized LogPosition beginning() {
      return LogPosition.wrap(reader.beginning());
    }

    @Override
    public synchronized LogPosition end() {
      return LogPosition.wrap(reader.ending());
    }

    @Override
    public synchronized LogPosition position(byte[] identity) {
      return LogPosition.wrap(log.position(identity));
    }

    @Override
    public void close() {
      // noop
    }

    private Log.Writer writer() {
      if (writer == null) {
        writer = new Log.Writer(log);
      }
      return writer;
    }

    private void invalidateWriter() {
      vars.invalidatedWriters.getAndIncrement();
      writer = null;
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
