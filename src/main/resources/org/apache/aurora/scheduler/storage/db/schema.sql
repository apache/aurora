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

CREATE TABLE quotas(
  id IDENTITY,
  role VARCHAR NOT NULL,
  num_cpus FLOAT NOT NULL,
  ram_mb INT NOT NULL,
  disk_mb INT NOT NULL,

  UNIQUE(role)
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
  host_attribute_id BIGINT NOT NULL REFERENCES host_attributes(id)
  ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(host_attribute_id, name, value)
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
  max_wait_to_instance_running_ms INT NOT NULL,
  min_wait_in_instance_running_ms INT NOT NULL,
  rollback_on_failure BOOLEAN NOT NULL,
  wait_for_batch_completion BOOLEAN NOT NULL,
  block_if_no_pulses_after_ms INT NULL,

  -- TODO(wfarner): Convert this to UNIQUE(job_key_id, update_id) to complete AURORA-1139.
  UNIQUE(update_id)
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
  task_config BINARY NOT NULL,
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

/**
 * NOTE: This table is truncated by TaskMapper, which will cause a conflict when the table is shared
 * with the forthcoming jobs table.  See note in TaskMapper about this before migrating MemJobStore.
 */
CREATE TABLE task_configs(
  id IDENTITY,
  job_key_id INT NOT NULL REFERENCES job_keys(id),
  creator_user VARCHAR NOT NULL,
  service BOOLEAN NOT NULL,
  num_cpus FLOAT8 NOT NULL,
  ram_mb INTEGER NOT NULL,
  disk_mb INTEGER NOT NULL,
  priority INTEGER NOT NULL,
  max_task_failures INTEGER NOT NULL,
  production BOOLEAN NOT NULL,
  contact_email VARCHAR,
  executor_name VARCHAR NOT NULL,
  executor_data VARCHAR NOT NULL
);

CREATE TABLE task_constraints(
  id IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,

  UNIQUE(task_config_id, name)
);

CREATE TABLE value_constraints(
  id IDENTITY,
  constraint_id INT NOT NULL REFERENCES task_constraints(id) ON DELETE CASCADE,
  negated BOOLEAN NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE value_constraint_values(
  id IDENTITY,
  value_constraint_id INT NOT NULL REFERENCES value_constraints(id) ON DELETE CASCADE,
  value VARCHAR NOT NULL,

  UNIQUE(value_constraint_id, value)
);

CREATE TABLE limit_constraints(
  id IDENTITY,
  constraint_id INT NOT NULL REFERENCES task_constraints(id) ON DELETE CASCADE,
  value INTEGER NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE task_config_requested_ports(
  id IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  port_name VARCHAR NOT NULL,

  UNIQUE(task_config_id, port_name)
);

CREATE TABLE task_config_task_links(
  id IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  label VARCHAR NOT NULL,
  url VARCHAR NOT NULL,

  UNIQUE(task_config_id, label)
);

CREATE TABLE task_config_metadata(
  id IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  key VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(task_config_id)
);

CREATE TABLE task_config_docker_containers(
  id IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  image VARCHAR NOT NULL,

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
  slave_row_id INT REFERENCES host_attributes(id),
  instance_id INTEGER NOT NULL,
  status INT NOT NULL REFERENCES task_states(id),
  failure_count INTEGER NOT NULL,
  ancestor_task_id VARCHAR NULL,
  task_config_row_id INT NOT NULL REFERENCES task_configs(id),

  UNIQUE(task_id)
);

CREATE TABLE task_events(
  id IDENTITY,
  task_row_id INT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  timestamp_ms BIGINT NOT NULL,
  status INT NOT NULL REFERENCES task_states(id),
  message VARCHAR NULL,
  scheduler_host VARCHAR NULL,
);

CREATE TABLE task_ports(
  id IDENTITY,
  task_row_id INT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  port INT NOT NULL,

  UNIQUE(task_row_id, name)
);