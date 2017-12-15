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
package org.apache.aurora.scheduler.storage.log;

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.log.SnapshotService.Settings;

/**
 * Binding for a snapshot store and period snapshotting service.
 */
public class SnapshotModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-dlog_snapshot_interval",
        description = "Specifies the frequency at which snapshots of local storage are taken and "
            + "written to the log.")
    public TimeAmount snapshotInterval = new TimeAmount(1, Time.HOURS);
  }

  private final Options options;

  public SnapshotModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    bind(Settings.class).toInstance(new Settings(options.snapshotInterval));
    bind(SnapshotStore.class).to(SnapshotService.class);
    bind(SnapshotService.class).in(Singleton.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(SnapshotService.class);
  }
}
