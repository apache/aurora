/**
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
package com.twitter.common.logging;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import com.google.common.base.Throwables;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author William Farner
 */
public class LogFormatterTest {

  private static final int THREAD_ID = 105;
  private static final long TIME_MILLIS = 1298065054839L;
  private static final String TIME_STRING = "0218 21:37:34.839";

  private LogFormatter formatter;

  @Before
  public void setUp() {
    formatter = new LogFormatter();
  }

  @Test
  public void testSimpleMessage() {
    String message = "Configurated the whizzbanger.";

    LogRecord record = makeRecord(Level.INFO, message);

    assertThat(formatter.format(record), is(
        String.format("I%s THREAD%d: %s\n", TIME_STRING, THREAD_ID, message)));
  }

  @Test
  public void testException() {
    String message = "The fuzzbizzer failed.";
    Throwable exception = new RuntimeException("No such fizzbuzzer.");

    LogRecord record = makeRecord(Level.WARNING, message);
    record.setThrown(exception);

    assertThat(formatter.format(record), is(
        String.format("W%s THREAD%d: %s\n%s\n", TIME_STRING, THREAD_ID, message,
            Throwables.getStackTraceAsString(exception))));
  }

  private static LogRecord makeRecord(Level level, String message) {
    LogRecord record = new LogRecord(level, message);
    record.setMillis(TIME_MILLIS);
    record.setThreadID(THREAD_ID);
    return record;
  }
}
