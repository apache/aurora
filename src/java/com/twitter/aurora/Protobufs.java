package com.twitter.aurora;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * Utility functions that are useful for working with protocol buffer messages.
 */
public final class Protobufs {

  private Protobufs() {
    // Utility class.
  }

  /**
   * Function to call {@link #toString(Message)} on message objects.
   */
  public static final Function<Message, String> SHORT_TOSTRING = new Function<Message, String>() {
    @Override public String apply(Message message) {
      return Protobufs.toString(message);
    }
  };

  /**
   * Alternative to the default protobuf toString implementation, which omits newlines.
   *
   * @param message Message to print.
   * @return String representation of the message.
   */
  public static String toString(Message message) {
    return TextFormat.shortDebugString(message);
  }
}
