import { invert } from 'utils/Common';

export const SCHEDULE_STATUS = invert(ScheduleStatus);

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

export default {
  OKAY_SCHEDULE_STATUS,
  WARNING_SCHEDULE_STATUS,
  ERROR_SCHEDULE_STATUS,
  USER_WAIT_SCHEDULE_STATUS
};
