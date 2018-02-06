import React from 'react';
import { shallow } from 'enzyme';

import Task from '../Task';

import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import TaskStatus from 'components/TaskStatus';

const TEST_CLUSTER = 'test-cluster';

const params = {
  role: 'test-role',
  environment: 'test-env',
  name: 'test-job',
  taskId: 'task-id1'
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
  status: ScheduleStatus.RUNNING
}];

function apiSpy() {
  return {
    getTasksWithoutConfigs: jest.fn()
  };
}

describe('Task', () => {
  it('Should render Loading before data is fetched', () => {
    expect(
      shallow(<Task api={apiSpy()} match={{params: params}} />).contains(<Loading />)).toBe(true);
  });

  it('Should render page elements when tasks are fetched', () => {
    const el = shallow(<Task api={createMockApi(tasks)} match={{params: params}} />);
    expect(el.contains(<Breadcrumb
      cluster={TEST_CLUSTER}
      env={params.environment}
      name={params.name}
      role={params.role}
      taskId={params.taskId} />)).toBe(true);
    expect(el.contains(<TaskStatus task={tasks[0]} title={`Task: ${params.taskId}`} />)).toBe(true);
  });
});
