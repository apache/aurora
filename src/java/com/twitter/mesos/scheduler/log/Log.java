package com.twitter.mesos.scheduler.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Represents an append only log that can be read after and truncated before a known
 * {@link Position}.
 *
 * <p>Logs are accessed by {@link #open() opening} a {@link Stream}.  All stream
 * access occurs with references to log entry {@link Position positions} in the stream.  These
 * positions can be remembered between runs of a client application by recording their
 * {@link Position#identity()} and {@link #position(byte[]) exchanging} it later for the original
 * position.
 *
 * @author John Sirois
 */
public interface Log {

  /**
   * An opaque ordered handle to a log entry's position in the log stream.
   */
  interface Position extends Comparable<Position> {

    /**
     * A unique value that can be exchanged with the {@link Log} for this position.  Useful for
     * saving checkpoints.
     *
     * @return this position's unique identity
     */
    byte[] identity();

  }

  /**
   * Represents a single entry in the log stream.
   */
  interface Entry {

    /**
     * All entries are guaranteed to have a position between {@link Stream#beginning()} and
     * {@link Stream#end()}.
     *
     * @return the position of this entry on the log stream
     */
    Position position();

    /**
     * @return the data stored in this log entry
     */
    byte[] contents();
  }

  /**
   * Indicates a {@link Position} that is not (currently) contained in this log stream.  This might
   * indicate the position id from a different log or that the position was from this log but has
   * been truncated.
   */
  class InvalidPositionException extends RuntimeException {
    public InvalidPositionException(String message) {
      super(message);
    }
    public InvalidPositionException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * An interface to the live {@link Log} stream that allows for appending, reading and writing
   * entries.
   */
  interface Stream extends Closeable {

    /**
     * Indicates a {@link Stream} could not be read from, written to or truncated due to some
     * underlying IO error.
     */
    class StreamAccessException extends RuntimeException {
      public StreamAccessException(String message, Throwable cause) {
        super(message, cause);
      }
    }

    /**
     * Appends an {@link Entry} to the end of the log stream.
     *
     * @param contents the data to store in the appended entry
     * @return the posiiton of the appended entry
     * @throws StreamAccessException if contents could not be appended to the stream
     */
    Position append(byte[] contents) throws StreamAccessException;

    /**
     * Allows reading log entries after a given {@code position}. To read all log entries, pass
     * {@link #beginning()}.  Implementations may materialize all entries after the the given
     * position at once or the may provide some form of streaming to back the returned entry
     * iterator.  If the implementation does use some form of streaming or batching, it may throw a
     * {@code StreamAccessException} on any call to {@link Iterator#hasNext()} or
     * {@link Iterator#next()}.
     *
     * @param position the position to read after
     * @return an iterator that ranges from the entry after the given {@code position} to the last
     *     entry in the log.
     * @throws InvalidPositionException if the specified position does not exist in this log
     * @throws StreamAccessException if the stream could not be read from
     */
    Iterator<Entry> readAfter(Position position)
        throws InvalidPositionException, StreamAccessException;

    /**
     * Removes all log entries preceding and including log entry at the given {@code position}. To
     * truncate all log entries, pass {@link #end()}.
     *
     * @param position the position of the latest entry to remove
     * @return the number of entries truncated
     * @throws InvalidPositionException if the specified position does not exist in this log
     * @throws StreamAccessException if the stream could not be truncated
     */
    long truncateTo(Position position) throws InvalidPositionException, StreamAccessException;

    /**
     * Returns a position that never points to any entry but is guaranteed to immediately precede
     * the first (oldest) log entry.
     *
     * @return the position immediately preceeding the first stream log entry
     */
    Position beginning();

    /**
     * Returns a position of the last (newest) log stream entry.
     *
     * @return the position immediately following the last stream log entry
     */
    Position end();

    /**
     * Returns a reasonable estimate of the current size of the log stream.  Depending on the
     * implementation and rate of log stream growth the result may deviate from the actual value
     * deemed reasonable for the log implementation.
     *
     * @return the current number of entries in the log stream
     */
    long size();
  }

  /**
   * Opens the log stream for reading writing and truncation.  Clients should ensure the stream is
   * closed when they are done using it.
   *
   * @return the log stream
   * @throws IOException if there was a problem opening the log stream
   */
  Stream open() throws IOException;

  /**
   * Exchanges an {@code identity} obtained from {@link Position#identity()} for the position
   * uniquely associated with it.  Useful for restoring saved checkpoints.
   *
   * @param identity the identity obtained from a prior call to {@link Position#identity()}
   * @return the corresponding position
   * @throws InvalidPositionException if identity does not represent a position produced by this log
   */
  Position position(byte[] identity) throws InvalidPositionException;
}
