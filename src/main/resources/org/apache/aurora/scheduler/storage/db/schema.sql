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

-- schema for h2 engine.
/* TODO(maxim): Consider using TIMESTAMP instead of BIGINT for all "timestamp" fields below. */

CREATE TABLE framework_id(
  id INT PRIMARY KEY,
  framework_id VARCHAR NOT NULL,

  UNIQUE(framework_id)
);

CREATE TABLE job_keys(
  id IDENTITY,
  role VARCHAR NOT NULL,
  environment VARCHAR NOT NULL,
  name VARCHAR NOT NULL,

  UNIQUE(role, environment, name)
);

CREATE TABLE locks(
  id IDENTITY,
  job_key_id BIGINT NOT NULL REFERENCES job_keys(id),
  token VARCHAR NOT NULL,
  user VARCHAR NOT NULL,
  timestampMs BIGINT NOT NULL,
  message VARCHAR,

  UNIQUE(job_key_id),
  UNIQUE(token)
);

CREATE TABLE maintenance_modes(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE host_attributes(
  id IDENTITY,
  host VARCHAR NOT NULL,
  mode INT NOT NULL REFERENCES maintenance_modes(id),
  slave_id VARCHAR NOT NULL,

  UNIQUE(host),
  UNIQUE(slave_id),
);

CREATE TABLE host_attribute_values(
  id IDENTITY,
  host_attribute_id BIGINT NOT NULL REFERENCES host_attributes(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(host_attribute_id, name, value)
);

/**
 * NOTE: This table is truncated by TaskMapper, which will cause a conflict when the table is shared
 * with the forthcoming jobs table.  See note in TaskMapper about this before migrating MemJobStore.
 */
CREATE TABLE task_configs(
  id IDENTITY,
  job_key_id BIGINT NOT NULL REFERENCES job_keys(id),
  creator_user VARCHAR NOT NULL,
  service BOOLEAN NOT NULL,
  num_cpus DOUBLE NOT NULL,
  ram_mb BIGINT NOT NULL,
  disk_mb BIGINT NOT NULL,
  priority INTEGER NOT NULL,
  max_task_failures INTEGER NOT NULL,
  production BOOLEAN NOT NULL,
  contact_email VARCHAR,
  executor_name VARCHAR,
  executor_data VARCHAR,
  tier VARCHAR
);

CREATE TABLE resource_types(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE task_resource(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  type_id INT NOT NULL REFERENCES resource_types(id),
  value VARCHAR NOT NULL,

  UNIQUE(task_config_id, type_id, value)
);

CREATE TABLE quotas(
  id IDENTITY,
  role VARCHAR NOT NULL,
  num_cpus DOUBLE NOT NULL,
  ram_mb BIGINT NOT NULL,
  disk_mb BIGINT NOT NULL,

  UNIQUE(role)
);

CREATE TABLE quota_resource(
  id IDENTITY,
  quota_id BIGINT NOT NULL REFERENCES quotas(id) ON DELETE CASCADE,
  type_id INT NOT NULL REFERENCES resource_types(id),
  value VARCHAR NOT NULL,

  UNIQUE(quota_id, type_id)
);

CREATE TABLE task_constraints(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,

  UNIQUE(task_config_id, name)
);

CREATE TABLE value_constraints(
  id IDENTITY,
  constraint_id BIGINT NOT NULL REFERENCES task_constraints(id) ON DELETE CASCADE,
  negated BOOLEAN NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE value_constraint_values(
  id IDENTITY,
  value_constraint_id BIGINT NOT NULL REFERENCES value_constraints(id) ON DELETE CASCADE,
  value VARCHAR NOT NULL,

  UNIQUE(value_constraint_id, value)
);

CREATE TABLE limit_constraints(
  id IDENTITY,
  constraint_id BIGINT NOT NULL REFERENCES task_constraints(id) ON DELETE CASCADE,
  value INTEGER NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE task_config_requested_ports(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  port_name VARCHAR NOT NULL,

  UNIQUE(task_config_id, port_name)
);

CREATE TABLE task_config_task_links(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  label VARCHAR NOT NULL,
  url VARCHAR NOT NULL,

  UNIQUE(task_config_id, label)
);

CREATE TABLE task_config_metadata(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  key VARCHAR NOT NULL,
  value VARCHAR NOT NULL
);

CREATE TABLE task_config_docker_containers(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  image VARCHAR NOT NULL,

  UNIQUE(task_config_id)
);

CREATE TABLE task_config_docker_container_parameters(
  id IDENTITY,
  container_id BIGINT NOT NULL REFERENCES task_config_docker_containers(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL
);

CREATE TABLE task_config_docker_images(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  tag VARCHAR NOT NULL,

  UNIQUE(task_config_id)
);

CREATE TABLE task_config_appc_images(
  id IDENTITY,
  task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  image_id VARCHAR NOT NULL,

  UNIQUE(task_config_id)
);

CREATE TABLE task_states(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE tasks(
  id IDENTITY,
  task_id VARCHAR NOT NULL,
  slave_row_id BIGINT REFERENCES host_attributes(id),
  instance_id INTEGER NOT NULL,
  status INT NOT NULL REFERENCES task_states(id),
  failure_count INTEGER NOT NULL,
  ancestor_task_id VARCHAR NULL,
  task_config_row_id BIGINT NOT NULL REFERENCES task_configs(id),

  UNIQUE(task_id)
);

CREATE TABLE task_events(
  id IDENTITY,
  task_row_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  timestamp_ms BIGINT NOT NULL,
  status INT NOT NULL REFERENCES task_states(id),
  message VARCHAR NULL,
  scheduler_host VARCHAR NULL,
);

CREATE TABLE task_ports(
  id IDENTITY,
  task_row_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  port INT NOT NULL,

  UNIQUE(task_row_id, name)
);

CREATE TABLE cron_policies(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE cron_jobs(
  id IDENTITY,
  job_key_id BIGINT NOT NULL REFERENCES job_keys(id),
  creator_user VARCHAR NOT NULL,
  cron_schedule VARCHAR NOT NULL,
  cron_collision_policy INT NOT NULL REFERENCES cron_policies(id),
  task_config_row_id BIGINT NOT NULL REFERENCES task_configs(id),
  instance_count INT NOT NULL,

  UNIQUE(job_key_id)
);

CREATE TABLE job_instance_update_actions(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE job_update_statuses(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE job_updates(
  id IDENTITY,
  job_key_id BIGINT NOT NULL REFERENCES job_keys(id),
  update_id VARCHAR NOT NULL,
  user VARCHAR NOT NULL,
  update_group_size INT NOT NULL,
  max_per_instance_failures INT NOT NULL,
  max_failed_instances INT NOT NULL,
  min_wait_in_instance_running_ms INT NOT NULL,
  rollback_on_failure BOOLEAN NOT NULL,
  wait_for_batch_completion BOOLEAN NOT NULL,
  block_if_no_pulses_after_ms INT NULL,

  UNIQUE(update_id, job_key_id)
);

CREATE TABLE job_update_locks(
  id IDENTITY,
  update_row_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  lock_token VARCHAR NOT NULL REFERENCES locks(token) ON DELETE CASCADE,

  UNIQUE(update_row_id),
  UNIQUE(lock_token)
);

CREATE TABLE job_update_configs(
  id IDENTITY,
  update_row_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  task_config_row_id BIGINT NOT NULL REFERENCES task_configs(id),
  is_new BOOLEAN NOT NULL
);

CREATE TABLE job_updates_to_instance_overrides(
  id IDENTITY,
  update_row_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  first INT NOT NULL,
  last INT NOT NULL,

  UNIQUE(update_row_id, first, last)
);

CREATE TABLE job_updates_to_desired_instances(
  id IDENTITY,
  update_row_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  first INT NOT NULL,
  last INT NOT NULL,

  UNIQUE(update_row_id, first, last)
);

CREATE TABLE job_update_configs_to_instances(
  id IDENTITY,
  config_id BIGINT NOT NULL REFERENCES job_update_configs(id) ON DELETE CASCADE,
  first INT NOT NULL,
  last INT NOT NULL,

  UNIQUE(config_id, first, last)
);

CREATE TABLE job_update_events(
  id IDENTITY,
  update_row_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  status INT NOT NULL REFERENCES job_update_statuses(id),
  timestamp_ms BIGINT NOT NULL,
  user VARCHAR,
  message VARCHAR
);

CREATE TABLE job_instance_update_events(
  id IDENTITY,
  update_row_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  action INT NOT NULL REFERENCES job_instance_update_actions(id),
  instance_id INT NOT NULL,
  timestamp_ms BIGINT NOT NULL
);
