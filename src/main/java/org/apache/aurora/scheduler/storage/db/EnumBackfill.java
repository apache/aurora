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
package org.apache.aurora.scheduler.storage.db;

import javax.inject.Inject;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.ScheduleStatus;

import org.apache.aurora.scheduler.resources.ResourceType;

import static java.util.Objects.requireNonNull;

public interface EnumBackfill {
  /**
   * Hydrates all of the enum tables in the database.
   */
  void backfill();

  class EnumBackfillImpl implements EnumBackfill {

    private final EnumValueMapper enumValueMapper;

    @Inject
    public EnumBackfillImpl(EnumValueMapper mapper) {
      this.enumValueMapper = requireNonNull(mapper);
    }

    @Override
    public void backfill() {
      for (CronCollisionPolicy policy : CronCollisionPolicy.values()) {
        enumValueMapper.addEnumValue("cron_policies", policy.getValue(), policy.name());
      }

      for (MaintenanceMode mode : MaintenanceMode.values()) {
        enumValueMapper.addEnumValue("maintenance_modes", mode.getValue(), mode.name());
      }

      for (JobUpdateStatus status : JobUpdateStatus.values()) {
        enumValueMapper.addEnumValue("job_update_statuses", status.getValue(), status.name());
      }

      for (JobUpdateAction jua : JobUpdateAction.values()) {
        enumValueMapper.addEnumValue("job_instance_update_actions", jua.getValue(), jua.name());
      }

      for (ScheduleStatus status : ScheduleStatus.values()) {
        enumValueMapper.addEnumValue("task_states", status.getValue(), status.name());
      }

      for (ResourceType type : ResourceType.values()) {
        enumValueMapper.addEnumValue("resource_types", type.getValue(), type.name());
      }

      for (Mode mode : Mode.values()) {
        enumValueMapper.addEnumValue("volume_modes", mode.getValue(), mode.name());
      }
    }
  }
}
