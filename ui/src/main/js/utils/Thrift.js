import { invert } from 'utils/Common';

export const SCHEDULE_STATUS = invert(ScheduleStatus);
export const UPDATE_STATUS = invert(JobUpdateStatus);
export const UPDATE_ACTION = invert(JobUpdateAction);

export const OKAY_SCHEDULE_STATUS = [
  ScheduleStatus.RUNNING,
  ScheduleStatus.FINISHED
];

export const WARNING_SCHEDULE_STATUS = [
  ScheduleStatus.ASSIGNED,
  ScheduleStatus.PENDING,
  ScheduleStatus.LOST,
  ScheduleStatus.KILLING,
  ScheduleStatus.DRAINING,
  ScheduleStatus.PREEMPTING
];

export const USER_WAIT_SCHEDULE_STATUS = [
  ScheduleStatus.STARTING
];

export const ERROR_SCHEDULE_STATUS = [
  ScheduleStatus.THROTTLED,
  ScheduleStatus.FAILED
];

export const OKAY_UPDATE_STATUS = [
  JobUpdateStatus.ROLLED_FORWARD
];

export const WARNING_UPDATE_STATUS = [
  JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
  JobUpdateStatus.ROLL_FORWARD_PAUSED
];

export const ERROR_UPDATE_STATUS = [
  JobUpdateStatus.ROLLING_BACK,
  JobUpdateStatus.ROLLED_BACK,
  JobUpdateStatus.ROLL_BACK_PAUSED,
  JobUpdateStatus.ABORTED,
  JobUpdateStatus.ERROR,
  JobUpdateStatus.FAILED,
  JobUpdateStatus.ROLL_BACK_AWAITING_PULSE
];

export const OKAY_UPDATE_ACTION = [
  JobUpdateAction.INSTANCE_UPDATED
];

export const WARNING_UPDATE_ACTION = [
  JobUpdateAction.INSTANCE_ROLLING_BACK,
  JobUpdateAction.INSTANCE_ROLLED_BACK
];

export const ERROR_UPDATE_ACTION = [
  JobUpdateAction.INSTANCE_UPDATE_FAILED,
  JobUpdateAction.INSTANCE_ROLLBACK_FAILED
];

export default {
  OKAY_SCHEDULE_STATUS,
  WARNING_SCHEDULE_STATUS,
  ERROR_SCHEDULE_STATUS,
  USER_WAIT_SCHEDULE_STATUS,
  OKAY_UPDATE_STATUS,
  WARNING_UPDATE_STATUS,
  ERROR_UPDATE_STATUS,
  OKAY_UPDATE_ACTION,
  WARNING_UPDATE_ACTION,
  ERROR_UPDATE_ACTION
};
