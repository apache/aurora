import moment from 'moment';
import { isNully } from 'utils/Common';
import ThriftUtils, { SCHEDULE_STATUS } from 'utils/Thrift';

export function isActive(task) {
  return ACTIVE_STATES.includes(task.status);
}

export function getClassForScheduleStatus(status) {
  if (ThriftUtils.OKAY_SCHEDULE_STATUS.includes(status)) {
    return 'okay';
  } else if (ThriftUtils.WARNING_SCHEDULE_STATUS.includes(status)) {
    return 'attention';
  } else if (ThriftUtils.ERROR_SCHEDULE_STATUS.includes(status)) {
    return 'error';
  } else if (ThriftUtils.USER_WAIT_SCHEDULE_STATUS.includes(status)) {
    return 'in-progress';
  }
  return 'system';
}

export function taskToStateMachine(task) {
  return task.taskEvents.map((e, i) => {
    const active = (i === task.taskEvents.length - 1) ? ' active' : '';
    return {
      timestamp: e.timestamp,
      className: `${getClassForScheduleStatus(e.status)}${active}`,
      state: SCHEDULE_STATUS[e.status],
      message: e.message
    };
  });
}

export function getLastEventTime(task) {
  if (task.taskEvents.length > 0) {
    return task.taskEvents[task.taskEvents.length - 1].timestamp;
  }
}

export function getDuration(task) {
  const firstEvent = moment(task.taskEvents[0].timestamp);
  const latestEvent = moment(task.taskEvents[task.taskEvents.length - 1].timestamp);
  return moment.duration(latestEvent.diff(firstEvent)).humanize();
}

export function instanceRangeToString(ranges) {
  return ranges.map(({first, last}) => (first === last) ? first : `${first} - ${last}`).join(', ');
}

export function getActiveResource(resource) {
  return Object.keys(resource).find((r) => !isNully(resource[r]));
}

export function constraintToString(constraint) {
  console.log(constraint);
  return isNully(constraint.value)
    ? `limit=${constraint.limit.limit}`
    : (constraint.value.negated ? '!' : '=') + constraint.value.values.join(',');
}

export function getResource(resources, key) {
  return resources.find((r) => !isNully(r[key]));
}

export function getResources(resources, key) {
  return resources.filter((r) => !isNully(r[key]));
}

export function isThermos(task) {
  return task && task.executorConfig && task.executorConfig.name === 'AuroraExecutor';
}
