import createBuilder from 'test-utils/Builder';

const INSTANCE_ID = 0;
const TASK_ID = 'test-task-id';
const SLAVE_ID = 'test-agent-id';
const HOST = 'test-host';
const TIER = 'preferred';
const ROLE = 'test-role';
const ENV = 'test-env';
const NAME = 'test-name';
const USER = 'user';

const JOB_KEY = {
  role: ROLE,
  environment: ENV,
  name: NAME
};

export default {
  HOST, INSTANCE_ID, SLAVE_ID, TASK_ID, ROLE, ENV, NAME, USER, JOB_KEY
};

export const TaskConfigBuilder = createBuilder({
  job: JOB_KEY,
  owner: {
    user: USER
  },
  isService: true,
  priority: 0,
  maxTaskFailures: 0,
  tier: TIER,
  resources: [{numCpus: 1}, {ramMb: 1024}, {diskMb: 1024}],
  constraints: [],
  requestedPorts: [],
  executorConfig: {data: {}, name: 'TestExecutor'}
});

export const AssignedTaskBuilder = createBuilder({
  taskId: TASK_ID,
  slaveId: SLAVE_ID,
  slaveHost: HOST,
  task: TaskConfigBuilder.build(),
  assignedPorts: {},
  instanceId: INSTANCE_ID
});

export const TaskEventBuilder = createBuilder({
  message: '',
  timestamp: 0,
  status: ScheduleStatus.PENDING
});

export const ScheduledTaskBuilder = createBuilder({
  assignedTask: AssignedTaskBuilder.build(),
  status: ScheduleStatus.PENDING,
  failureCount: 0,
  taskEvents: [TaskEventBuilder.build()],
  ancestorId: ''
});

export function createConfigGroup(taskBuilder, ...instances) {
  return {
    config: taskBuilder.build(),
    instances: instances.map((pair) => { return {first: pair[0], last: pair[1]}; })
  };
}
