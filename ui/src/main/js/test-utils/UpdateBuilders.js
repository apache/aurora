import createBuilder from 'test-utils/Builder';

import { TaskConfigBuilder } from './TaskBuilders';

const USER = 'update-user';
const UPDATE_ID = 'update-id';
const JOB_KEY = {
  role: 'test-role',
  environment: 'test-env',
  name: 'test-name'
};
const UPDATE_KEY = {
  job: JOB_KEY,
  id: UPDATE_ID
};

export default {
  USER
};

export const UpdateSettingsBuilder = createBuilder({
  updateGroupSize: 1,
  maxPerInstanceFailures: 0,
  maxFailedInstances: 0,
  minWaitInInstanceRunningMs: 1,
  rollbackOnFailure: true,
  updateOnlyTheseInstances: [],
  waitForBatchCompletion: false
});

export const UpdateEventBuilder = createBuilder({
  status: JobUpdateStatus.ROLLING_FORWARD,
  timestampMs: 0,
  user: USER,
  message: ''
});

export const InstanceUpdateEventBuilder = createBuilder({
  instanceId: 0,
  timestampMs: 0,
  action: JobUpdateAction.INSTANCE_UPDATING
});

export const InstanceTaskConfigBuilder = createBuilder({
  task: TaskConfigBuilder.build(),
  instances: [{first: 0, last: 0}]
});

export const UpdateInstructionsBuilder = createBuilder({
  initialState: [InstanceTaskConfigBuilder.build()],
  desiredState: InstanceTaskConfigBuilder.task(
    TaskConfigBuilder.resources([{numCpus: 2, ramMb: 2048, diskMb: 2048}])).build(),
  settings: UpdateSettingsBuilder.build()
});

export const UpdateStateBuilder = createBuilder({
  status: JobUpdateStatus.ROLLING_FORWARD,
  createdTimestampMs: 0,
  lastModifiedTimestampMs: 60000
});

export const UpdateSummaryBuilder = createBuilder({
  key: UPDATE_KEY,
  user: USER,
  state: UpdateStateBuilder.build(),
  metadata: []
});

export const UpdateBuilder = createBuilder({
  summary: UpdateSummaryBuilder.build(),
  instructions: UpdateInstructionsBuilder.build()
});

export const UpdateDetailsBuilder = createBuilder({
  update: UpdateBuilder.build(),
  updateEvents: [UpdateEventBuilder.build()],
  instanceEvents: [InstanceUpdateEventBuilder.build()]
});

export function builderWithStatus(updateStatus) {
  return UpdateDetailsBuilder.update(
    UpdateBuilder.summary(
      UpdateSummaryBuilder.state(UpdateStateBuilder.status(updateStatus).build()).build()
    ).build()
  ).updateEvents([UpdateEventBuilder.status(updateStatus).build()]);
}
