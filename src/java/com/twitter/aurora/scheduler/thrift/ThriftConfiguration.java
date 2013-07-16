package com.twitter.aurora.scheduler.thrift;

import java.io.IOException;
import java.io.InputStream;

/**
 * Container for thrift server configuration options.
 */
public interface ThriftConfiguration {
  /**
   * Gets a stream for the thrift socket SSL key.
   *
   * @return A stream that contains the SSL key data.
   * @throws IOException If the stream could not be opened.
   */
  InputStream getSslKeyStream() throws IOException;

  /**
   * Gets the port that the thrift server should listen on.
   *
   * @return Thrift server port.
   */
  int getServingPort();
}
