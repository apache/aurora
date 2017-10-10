// setup file
import { configure } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';

configure({ adapter: new Adapter() });

// Forced to do this because of how Thrift is wired into the UI. Namely that
// the jQuery/web-based Thrift generated code writes things into global namespace - but omits
// the var from the variable assignment (otherwise we could just eval the file here to load
// it into the global namespace). Unfortunately it means our Thrift unit tests
// can fall out of sync with API changes - but I don't see a way around this that isn't brittle
// to changes to the API anyway.
global.ScheduleStatus = {
  'INIT' : 11,
  'THROTTLED' : 16,
  'PENDING' : 0,
  'ASSIGNED' : 9,
  'STARTING' : 1,
  'RUNNING' : 2,
  'FINISHED' : 3,
  'PREEMPTING' : 13,
  'RESTARTING' : 12,
  'DRAINING' : 17,
  'FAILED' : 4,
  'KILLED' : 5,
  'KILLING' : 6,
  'LOST' : 7
};
global.ACTIVE_STATES = [9,17,6,0,13,12,2,1,16];

global.TaskQuery = () => {};
global.JobKey = () => {};
global.JobUpdateKey = () => {};
global.JobUpdateQuery = () => {};

global.JobUpdateStatus = {
  'ROLLING_FORWARD' : 0,
  'ROLLING_BACK' : 1,
  'ROLL_FORWARD_PAUSED' : 2,
  'ROLL_BACK_PAUSED' : 3,
  'ROLLED_FORWARD' : 4,
  'ROLLED_BACK' : 5,
  'ABORTED' : 6,
  'ERROR' : 7,
  'FAILED' : 8,
  'ROLL_FORWARD_AWAITING_PULSE' : 9,
  'ROLL_BACK_AWAITING_PULSE' : 10
};

global.ACTIVE_JOB_UPDATE_STATES = [
  JobUpdateStatus.ROLLING_FORWARD,
  JobUpdateStatus.ROLLING_BACK,
  JobUpdateStatus.ROLL_FORWARD_PAUSED,
  JobUpdateStatus.ROLL_BACK_PAUSED,
  JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
  JobUpdateStatus.ROLL_BACK_AWAITING_PULSE
];

global.JobUpdateAction = {
  'INSTANCE_UPDATED' : 1,
  'INSTANCE_ROLLED_BACK' : 2,
  'INSTANCE_UPDATING' : 3,
  'INSTANCE_ROLLING_BACK' : 4,
  'INSTANCE_UPDATE_FAILED' : 5,
  'INSTANCE_ROLLBACK_FAILED' : 6
};
