-- schema for h2 engine.

CREATE TABLE job_keys(
  id INT IDENTITY,
  role VARCHAR NOT NULL,
  environment VARCHAR NOT NULL,
  name VARCHAR NOT NULL,

  UNIQUE(role, environment, name)
);

CREATE TABLE locks(
  id INT IDENTITY,
  job_key_id INT NOT NULL REFERENCES job_keys(id),
  token VARCHAR NOT NULL,
  user VARCHAR NOT NULL,
  timestampMs BIGINT NOT NULL,
  message VARCHAR,

  UNIQUE(job_key_id)
);

CREATE TABLE quotas(
  id INT IDENTITY,
  role VARCHAR NOT NULL,
  num_cpus FLOAT NOT NULL,
  ram_mb INT NOT NULL,
  disk_mb INT NOT NULL,

  UNIQUE(role)
);
