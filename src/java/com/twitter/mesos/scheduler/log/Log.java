package com.twitter.mesos.scheduler.log;

import java.util.Iterator;

import com.google.common.base.Preconditions;

/**
 * Represents an append only log that can be read after and truncated before a known
 * {@link Position}.
 *
 * @author John Sirois
 */
public interface Log {

  /**
   * An opaque ordered handle to a log entry's position in the log stream.
   */
  interface Position extends Comparable<Position> {

    /**
     * A position that never points to any entry but is guaranteed to immediately precede the first
     * (oldest) log entry.
     */
    Position BEGINNING = new Position() {
      @Override public int compareTo(Position o) {
        return this == o ? 0 : -1 /* BEGINNING is always first */;
      }
      @Override public String toString() {
        return "BEGINNING";
      }
    };

    /**
     * A position that never points to any entry but is guaranteed to immediately follow the last
     * (newest) log entry.
     */
    Position END = new Position() {
      @Override public int compareTo(Position o) {
        return this == o ? 0 : +1 /* END is always last */;
      }
      @Override public String toString() {
        return "END";
      }
    };

    /**
     * A base class suitable for position implementations that are not intended for subclassing
     * themselves.
     *
     * @param <T> the type of the the subclass itself
     */
    abstract class Base<T extends Base> implements Position {
      private final Class<T> type;

      protected Base(Class<T> type) {
        this.type = Preconditions.checkNotNull(type);
      }

      @Override
      public final int compareTo(Position other) {
        if (type != other.getClass()) {
          return -1 * other.compareTo(this);
        }
        return compareWith(type.cast(other));
      }

      /**
       * Equivalent to {@link Comparable#compareTo(Object)} except that instances are guaranteed
       * to be of this subclasses exact type.  This ensures ordering with respect to
       * {@link Position#BEGINNING} and {@link Position#END} is always correct.
       *
       * @param other another position of the same type as the subclass
       * @return a negative integer, zero, or a positive integer as this object
       *     is less than, equal to, or greater than the {@code other} object.
       */
      protected abstract int compareWith(T other);

      @Override
      public final boolean equals(Object other) {
        return (type == other.getClass()) && equalTo(type.cast(other));
      }

      /**
       * Equivalent to {@link Object#equals(Object)} except that instances are guaranteed
       * to be of this subclasses exact type.  This ensures instances of this subclass type will
       * never be considered equal to {@link Position#BEGINNING} or {@link Position#END}.
       *
       * @param other another position of the same type as the subclass
       * @return true if other is equivalent to this object
       */
      protected abstract boolean equalTo(T other);
    }
  }

  /**
   * Represents a single entry in the log stream.
   */
  interface Entry {

    /**
     * All entries are guaranteed to have a position between {@link Position#BEGINNING} and
     * {@link Position#END}.
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
  }

  /**
   * Appends an {@link Entry} to the end of the log stream.
   *
   * @param contents the data to store in the appended entry
   * @return the posiiton of the appended entry
   */
  Position append(byte[] contents);

  /**
   * Allows reading log entries after a given {@code position}. To read all log entries, pass
   * {@link Position#BEGINNING}.
   *
   * @param position the position of the oldest entry to read
   * @return an iterator that ranges from the entry at the given {@code position} to the last entry
   *     in the log.
   * @throws InvalidPositionException if the specified position does not exist in this log
   */
  Iterator<Entry> readAfter(Position position) throws InvalidPositionException;

  /**
   * Removes all log entries preceding and the log entry at the given {@code position}. To truncate
   * all log entries, pass {@link Position#END}.
   *
   * @param position the position of the latest entry to remove
   * @return the number of entries truncated
   * @throws InvalidPositionException if the specified position does not exist in this log
   */
  long truncateBefore(Position position) throws InvalidPositionException;

  /**
   * Returns a reasonable estimate of the current size of the log stream.  Depending on the
   * implementation and rate of log stream growth the result may deviate from the actual value
   * deemed reasonable for the log implementation.
   *
   * @return the current number of entries in the log stream
   */
  long size();
}
