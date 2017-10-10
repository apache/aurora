import moment from 'moment';
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
