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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default handler to invoke when the process is being instructed to exit immediately.
 */
class AbortCallback implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AbortCallback.class);

  @Override public void run() {
    LOG.info("ABORTING PROCESS IMMEDIATELY!");
    Runtime.getRuntime().halt(0);
  }
}
