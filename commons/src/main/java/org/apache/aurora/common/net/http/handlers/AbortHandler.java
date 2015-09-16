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
package org.apache.aurora.common.net.http.handlers;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * A servlet that provides a way to remotely terminate the running process immediately.
 */
@Path("/abortabortabort")
public class AbortHandler {

  /**
   * A {@literal @Named} binding key for the QuitHandler listener.
   */
  public static final String ABORT_HANDLER_KEY =
      "com.twitter.common.net.http.handlers.AbortHandler.listener";

  private static final Logger LOG = Logger.getLogger(AbortHandler.class.getName());

  private final Runnable abortListener;

  /**
   * Constructs a new AbortHandler that will notify the given {@code abortListener} when the servlet
   * is accessed.  It is the responsibility of the listener to initiate an immediate shutdown of
   * the system.
   *
   * @param abortListener Runnable to notify when the servlet is accessed.
   */
  @Inject
  public AbortHandler(@Named(ABORT_HANDLER_KEY) Runnable abortListener) {
    this.abortListener = Preconditions.checkNotNull(abortListener);
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public void abort(@Context HttpServletRequest req) throws IOException {
    LOG.info(String.format("Received abort HTTP signal from %s (%s)",
        req.getRemoteAddr(), req.getRemoteHost()));

    abortListener.run();
  }
}
