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