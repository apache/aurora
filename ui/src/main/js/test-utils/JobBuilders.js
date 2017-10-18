import createBuilder from 'test-utils/Builder';

import { TaskConfigBuilder } from './TaskBuilders';

const ROLE = 'test-role';
const ENV = 'test-env';
const NAME = 'test-name';
const USER = 'test-user';

const JOB_KEY = {
  role: ROLE,
  environment: ENV,
  name: NAME
};

export const JobConfigurationBuilder = createBuilder({
  key: JOB_KEY,
  owner: {user: USER},
  cronSchedule: null,
  cronCollisionPolicy: 0,
  taskConfig: TaskConfigBuilder.build(),
  instanceCount: 1
});

export const JobStatsBuilder = createBuilder({
  activeTaskCount: 0,
  pendingTaskCount: 0,
  finishedTaskCount: 0,
  failedTaskCount: 0
});

export const JobSummaryBuilder = createBuilder({
  job: JobConfigurationBuilder.build(),
  stats: JobStatsBuilder.build(),
  nextCronRunMs: null
});
