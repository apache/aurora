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
package org.apache.aurora.scheduler.http;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.common.util.Clock;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.DateCache;
import org.eclipse.jetty.util.component.AbstractLifeCycle;

import static java.util.Objects.requireNonNull;

/**
 * A copy of twitter RequestLogger from twitter commons, which is a port of jetty's NCSARequestLog
 * to use java.util.logging.  This clone exists to allow us to upgrade to jetty 7.
 * <p>
 * TODO(wfarner): Replace this with jetty's Slf4jRequestLog once we upgrade to jetty 8.
 */
public class RequestLogger extends AbstractLifeCycle implements RequestLog {

  private static final Logger LOGGER = Logger.getLogger(RequestLogger.class.getName());

  private final Clock clock;
  private final LogSink sink;
  private final DateCache logDateCache;

  interface LogSink {
    boolean isLoggable(Level level);
    void log(Level level, String messagge);
  }

  RequestLogger() {
    this(Clock.SYSTEM_CLOCK, new LogSink() {
      @Override
      public boolean isLoggable(Level level) {
        return LOGGER.isLoggable(level);
      }

      @Override public void log(Level level, String message) {
        LOGGER.log(level, message);
      }
    });
  }

  @VisibleForTesting
  RequestLogger(Clock clock, LogSink sink) {
    this.clock = requireNonNull(clock);
    this.sink = requireNonNull(sink);
    logDateCache = new DateCache("dd/MMM/yyyy:HH:mm:ss Z", Locale.getDefault());
    logDateCache.setTimeZoneID("GMT");
  }

  private String formatEntry(Request request, Response response) {
    StringBuilder buf = new StringBuilder();

    buf.append(request.getServerName());
    buf.append(' ');

    String addr = request.getHeader(HttpHeaders.X_FORWARDED_FOR);
    if (addr == null) {
      addr = request.getRemoteAddr();
    }

    buf.append(addr);
    buf.append(" [");
    buf.append(logDateCache.format(request.getTimeStamp()));
    buf.append("] \"");
    buf.append(request.getMethod());
    buf.append(' ');
    buf.append(request.getUri().toString());
    buf.append(' ');
    buf.append(request.getProtocol());
    buf.append("\" ");
    buf.append(response.getStatus());
    buf.append(' ');
    buf.append(response.getContentCount());
    buf.append(' ');

    String referer = request.getHeader(HttpHeaders.REFERER);
    if (referer == null) {
      buf.append("\"-\" ");
    } else {
      buf.append('"');
      buf.append(referer);
      buf.append("\" ");
    }

    String agent = request.getHeader(HttpHeaders.USER_AGENT);
    if (agent == null) {
      buf.append("\"-\" ");
    } else {
      buf.append('"');
      buf.append(agent);
      buf.append('"');
    }

    buf.append(' ');
    buf.append(clock.nowMillis() - request.getTimeStamp());
    return buf.toString();
  }

  @Override
  public void log(Request request, Response response) {
    int statusCategory = response.getStatus() / 100;
    Level level = statusCategory == 2 || statusCategory == 3 ? Level.INFO : Level.WARNING;
    if (!sink.isLoggable(level)) {
      return;
    }

    sink.log(level, formatEntry(request, response));
  }
}
