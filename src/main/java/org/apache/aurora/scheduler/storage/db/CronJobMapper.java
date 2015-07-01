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

import java.util.List;

import javax.annotation.Nullable;

import org.apache.aurora.scheduler.storage.db.views.CronJobWrapper;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper for cron jobs.
 */
interface CronJobMapper {

  void insert(@Param("job") IJobConfiguration job, @Param("task_config_id") long taskConfigId);

  void delete(@Param("job") IJobKey job);

  void truncate();

  List<CronJobWrapper> selectAll();

  @Nullable
  CronJobWrapper select(@Param("job") IJobKey job);
}
