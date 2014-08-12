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

  UNIQUE(job_key_id)
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
  instance_count INT NOT NULL,
  update_group_size INT NOT NULL,
  max_per_instance_failures INT NOT NULL,
  max_failed_instances INT NOT NULL,
  max_wait_to_instance_running_ms INT NOT NULL,
  min_wait_in_instance_running_ms INT NOT NULL,
  rollback_on_failure BOOLEAN NOT NULL,

  UNIQUE(update_id)
);

CREATE TABLE job_update_configs(
  id IDENTITY,
  update_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  task_config BINARY NOT NULL,
  is_new BOOLEAN NOT NULL
);

CREATE TABLE job_updates_to_instance_overrides(
  id IDENTITY,
  update_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  first INT NOT NULL,
  last INT NOT NULL,

  UNIQUE(update_id, first, last)
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
  update_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  status INT NOT NULL REFERENCES job_update_statuses(id),
  timestamp_ms BIGINT NOT NULL
);

CREATE TABLE job_instance_update_events(
  id IDENTITY,
  update_id BIGINT NOT NULL REFERENCES job_updates(id) ON DELETE CASCADE,
  action INT NOT NULL REFERENCES job_instance_update_actions(id),
  instance_id INT NOT NULL,
  timestamp_ms BIGINT NOT NULL
);
