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
package org.apache.aurora.scheduler.updater;

import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;

/**
 * TODO(wfarner): Implement this as part of AURORA-610.
 */
public class JobUpdateControllerImpl implements JobUpdateController {
  @Override
  public void start(IJobUpdate update) throws UpdateStateException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void pause(IJobKey job) throws UpdateStateException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void resume(IJobKey job) throws UpdateStateException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void abort(IJobKey job) throws UpdateStateException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void instanceChangedState(IInstanceKey instance) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void systemResume(IJobKey job) throws UpdateStateException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }
}
