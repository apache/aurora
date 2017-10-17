import React from 'react';
import { shallow } from 'enzyme';

import Job from '../Job';

import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import Tabs from 'components/Tabs';
import { JobUpdateList } from 'components/UpdateList';
import UpdatePreview from 'components/UpdatePreview';

import { ScheduledTaskBuilder } from 'test-utils/TaskBuilders';
import { builderWithStatus } from 'test-utils/UpdateBuilders';

const params = {
  role: 'test-role',
  environment: 'test-env',
  name: 'test-job'
};

function apiSpy() {
  return {
    getTasksWithoutConfigs: jest.fn(),
    getPendingReason: jest.fn(),
    getConfigSummary: jest.fn(),
    getJobUpdateDetails: jest.fn()
  };
}

describe('Job', () => {
  // basic props to force render of all components
  const props = (tasks = []) => {
    return {
      api: apiSpy(),
      cluster: 'test',
      history: jest.fn(),
      location: {},
      match: {params: params},
      tasks: tasks
    };
  };

  it('Should render Loading and fire off calls for data', () => {
    const api = apiSpy();
    expect(shallow(<Job api={api} match={{params: params}} />)
      .contains(<Loading />)).toBe(true);
    Object.keys(api).forEach((apiKey) => expect(api[apiKey].mock.calls.length).toBe(1));
  });

  it('Should render breadcrumb with correct values', () => {
    const el = shallow(<Job {...props()} />);
    expect(el.contains(<Breadcrumb
      cluster='test'
      env={params.environment}
      name={params.name}
      role={params.role} />)).toBe(true);
  });

  it('Should show UpdatePreview if in-progress update exists', () => {
    const updates = [builderWithStatus(JobUpdateStatus.ROLLING_FORWARD).build()];
    const el = shallow(<Job {...props()} updates={updates} />);
    expect(el.contains(<UpdatePreview update={updates[0]} />)).toBe(true);
  });

  it('Should not show UpdatePreview if no in-progress update exists', () => {
    const updates = [builderWithStatus(JobUpdateStatus.ROLLED_FORWARD).build()];
    const el = shallow(<Job {...props()} updates={updates} />);
    expect(el.find(UpdatePreview).length).toBe(0);
  });

  it('Should render JobUpdateList with any terminal update summaries', () => {
    const updates = [
      builderWithStatus(JobUpdateStatus.ROLLING_FORWARD).build(),
      builderWithStatus(JobUpdateStatus.ROLLED_FORWARD).build(),
      builderWithStatus(JobUpdateStatus.ROLLED_BACK).build()
    ];
    const el = shallow(<Job {...props()} updates={updates} />);
    expect(el.contains(<JobUpdateList
      updates={[updates[1].update.summary, updates[2].update.summary]} />)).toBe(true);
  });

  it('Should not render JobUpdateList if no terminal updates', () => {
    const updates = [builderWithStatus(JobUpdateStatus.ROLLING_FORWARD).build()];
    const el = shallow(<Job {...props()} updates={updates} />);
    expect(el.find(JobUpdateList).length).toBe(0);
  });

  it('Should render task list with active tasks only on Job Status tab', () => {
    const tasks = [
      ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
      ScheduledTaskBuilder.status(ScheduleStatus.FINISHED).build()
    ];
    const el = shallow(<Job {...props(tasks)} />);
    const taskList = el.find(Tabs).props().tabs[0].content.props.children.props.tabs[0].content;
    expect(taskList.props.tasks).toEqual([tasks[0]]);
  });

  it('Should render task list with terminal tasks only on Job History tab', () => {
    const tasks = [
      ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
      ScheduledTaskBuilder.status(ScheduleStatus.FINISHED).build()
    ];
    const el = shallow(<Job {...props(tasks)} />);
    expect(el.find(Tabs).props().tabs[1].name).toEqual('Job History (1)');
    const taskList = el.find(Tabs).props().tabs[1].content.props.children;
    expect(taskList.props.tasks).toEqual([tasks[1]]);
  });

  it('Should set default active tabs based on URL query parameters', () => {
    const tasks = [
      ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
      ScheduledTaskBuilder.status(ScheduleStatus.FINISHED).build()
    ];
    const p = props(tasks);
    p.location.search = '?taskView=config&jobView=history';
    const el = shallow(<Job {...p} />);
    expect(el.find(Tabs).props().activeTab).toEqual('history');
    const nestedTab = el.find(Tabs).props().tabs[0].content.props.children.props.activeTab;
    expect(nestedTab).toEqual('config');
  });
});
