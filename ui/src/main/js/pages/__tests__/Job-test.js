import React from 'react';
import { shallow } from 'enzyme';

import Job from '../Job';

import Breadcrumb from 'components/Breadcrumb';
import { JobUpdateList } from 'components/UpdateList';
import UpdatePreview from 'components/UpdatePreview';

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
    getJobUpdateDetails: jest.fn(),
    getJobSummary: jest.fn()
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

  it('Should fire off calls for data', () => {
    const api = apiSpy();
    const apiProps = props();
    apiProps.api = api;
    shallow(<Job {...apiProps} />);
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
      updates={[updates[1], updates[2]]} />)).toBe(true);
  });

  it('Should not render JobUpdateList if no terminal updates', () => {
    const updates = [builderWithStatus(JobUpdateStatus.ROLLING_FORWARD).build()];
    const el = shallow(<Job {...props()} updates={updates} />);
    expect(el.find(JobUpdateList).length).toBe(0);
  });
});
