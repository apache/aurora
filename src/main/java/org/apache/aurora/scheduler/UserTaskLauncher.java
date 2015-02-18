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
package org.apache.aurora.scheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.OfferManager;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskStatus;

import static java.util.Objects.requireNonNull;

/**
 * A task launcher that matches resource offers against user tasks.
 */
class UserTaskLauncher implements TaskLauncher {

  private static final Logger LOG = Logger.getLogger(UserTaskLauncher.class.getName());

  @VisibleForTesting
  static final String MEMORY_LIMIT_EXCEEDED = "MEMORY STATISTICS";

  @VisibleForTesting
  static final String MEMORY_LIMIT_DISPLAY = "Task used more memory than requested.";

  private final Storage storage;
  private final OfferManager offerManager;
  private final StateManager stateManager;

  @Inject
  UserTaskLauncher(Storage storage, OfferManager offerManager, StateManager stateManager) {
    this.storage = requireNonNull(storage);
    this.offerManager = requireNonNull(offerManager);
    this.stateManager = requireNonNull(stateManager);
  }

  @Override
  public boolean willUse(HostOffer offer) {
    requireNonNull(offer);

    offerManager.addOffer(offer);
    return true;
  }

  @Override
  public synchronized boolean statusUpdate(final TaskStatus status) {
    @Nullable String message = null;
    if (status.hasMessage()) {
      message = status.getMessage();
    }

    try {
      final ScheduleStatus translatedState = Conversions.convertProtoState(status.getState());
      // TODO(William Farner): Remove this hack once Mesos API change is done.
      //                       Tracked by: https://issues.apache.org/jira/browse/MESOS-343
      if (translatedState == ScheduleStatus.FAILED
          && message != null
          && message.contains(MEMORY_LIMIT_EXCEEDED)) {
        message = MEMORY_LIMIT_DISPLAY;
      }

      final String auditMessage = message;
      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        protected void execute(Storage.MutableStoreProvider storeProvider) {
          stateManager.changeState(
              storeProvider,
              status.getTaskId().getValue(),
              Optional.<ScheduleStatus>absent(),
              translatedState,
              Optional.fromNullable(auditMessage));
        }
      });
    } catch (SchedulerException e) {
      LOG.log(Level.WARNING, "Failed to update status for: " + status, e);
      throw e;
    }
    return true;
  }

  @Override
  public void cancelOffer(OfferID offer) {
    offerManager.cancelOffer(offer);
  }
}
