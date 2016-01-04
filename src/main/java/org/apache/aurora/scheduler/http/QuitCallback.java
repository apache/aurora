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

import com.google.inject.Inject;

import org.apache.aurora.common.application.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP quit callback, which invokes {@link Lifecycle#shutdown()}.
 */
public class QuitCallback implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(QuitCallback.class);

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
