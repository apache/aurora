import React from 'react';
import { shallow } from 'enzyme';

import Instance from '../Instance';

import Breadcrumb from 'components/Breadcrumb';
import InstanceHistory from 'components/InstanceHistory';
import Loading from 'components/Loading';
import TaskStatus from 'components/TaskStatus';

const TEST_CLUSTER = 'test-cluster';

const params = {
  role: 'test-role',
  environment: 'test-env',
  name: 'test-job',
  instance: '1'
};

function createMockApi(tasks) {
  const api = {};
  api.getTasksWithoutConfigs = (query, handler) => handler({
    result: {
      scheduleStatusResult: {
        tasks: tasks
      }
    },
    serverInfo: {
      clusterName: TEST_CLUSTER
    }
  });
  return api;
}

const tasks = [{
  status: ScheduleStatus.FAILED
}, {
  status: ScheduleStatus.RUNNING
}, {
  status: ScheduleStatus.KILLED
}];

describe('Instance', () => {
  it('Should render Loading before data is fetched', () => {
    expect(shallow(<Instance
      api={{getTasksWithoutConfigs: () => {}}}
      match={{params: params}} />).contains(<Loading />)).toBe(true);
  });

  it('Should render page elements when tasks are fetched', () => {
    const el = shallow(<Instance api={createMockApi(tasks)} match={{params: params}} />);
    expect(el.contains(<Breadcrumb
      cluster={TEST_CLUSTER}
      env={params.environment}
      instance={params.instance}
      name={params.name}
      role={params.role} />)).toBe(true);
    expect(el.contains(<TaskStatus task={tasks[1]} />)).toBe(true);
    expect(el.contains(<InstanceHistory tasks={[tasks[0], tasks[2]]} />)).toBe(true);
  });
});
