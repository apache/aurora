/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
