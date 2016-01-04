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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet that provides a way to remotely signal the process to initiate a clean shutdown
 * sequence.
 */
@Path("/quitquitquit")
public class QuitHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QuitHandler.class);

  /**
   * A {@literal @Named} binding key for the QuitHandler listener.
   */
  public static final String QUIT_HANDLER_KEY =
      "com.twitter.common.net.http.handlers.QuitHandler.listener";

  private final Runnable quitListener;

  /**
   * Constructs a new QuitHandler that will notify the given {@code quitListener} when the servlet
   * is accessed.  It is the responsibility of the listener to initiate a clean shutdown of the
   * process.
   *
   * @param quitListener Runnable to notify when the servlet is accessed.
   */
  @Inject
  public QuitHandler(@Named(QUIT_HANDLER_KEY) Runnable quitListener) {
    this.quitListener = Preconditions.checkNotNull(quitListener);
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public String quit(@Context HttpServletRequest req) {
    LOG.info("Received quit HTTP signal from {} ({})", req.getRemoteAddr(), req.getRemoteHost());

    new Thread(quitListener).start();
    return "Notifying quit listener.";
  }
}
