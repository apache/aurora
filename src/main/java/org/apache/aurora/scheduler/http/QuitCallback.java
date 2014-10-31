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

import java.util.logging.Logger;

import com.google.inject.Inject;
import com.twitter.common.application.Lifecycle;

/**
 * HTTP quit callback, which invokes {@link Lifecycle#shutdown()}.
 */
public class QuitCallback implements Runnable {

  private static final Logger LOG = Logger.getLogger(QuitCallback.class.getName());

  private final Lifecycle lifecycle;

  @Inject
  public QuitCallback(Lifecycle lifecycle) {
    this.lifecycle = lifecycle;
  }

  @Override
  public void run() {
    LOG.info("Instructing lifecycle to shut down.");
    lifecycle.shutdown();
  }
}
