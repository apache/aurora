import React from 'react';
import { shallow } from 'enzyme';

import Update from '../Update';

import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import UpdateConfig from 'components/UpdateConfig';
import UpdateDetails from 'components/UpdateDetails';

const TEST_CLUSTER = 'test-cluster';

const params = {
  role: 'test-role',
  environment: 'test-env',
  name: 'test-job',
  instance: '1',
  uid: 'update-id'
};

function createMockApi(update) {
  const api = {};
  const mockGetJobUpdateDetails = jest.fn().mockImplementation(
    (id, query, handler) => handler({
      result: {
        getJobUpdateDetailsResult: {
          detailsList: [update]
        }
      },
      serverInfo: {
        clusterName: TEST_CLUSTER
      }
    })
  );
  api.getJobUpdateDetails = mockGetJobUpdateDetails;
  return api;
}

const update = {update: {summary: {state: {status: JobUpdateStatus.ROLLING_FORWARD}}}};

describe('Update', () => {
  it('Should render Loading before data is fetched', () => {
    expect(shallow(<Update
      api={{getJobUpdateDetails: () => {}}}
      match={{params: params}} />).contains(<Loading />)).toBe(true);
  });

  it('Should render page elements when update is fetched', () => {
    const el = shallow(<Update api={createMockApi(update)} match={{params: params}} />);
    expect(el.contains(<Breadcrumb
      cluster={TEST_CLUSTER}
      env={params.environment}
      name={params.name}
      role={params.role}
      update={params.uid} />)).toBe(true);
    expect(el.contains(<UpdateConfig update={update} />)).toBe(true);
    expect(el.contains(<UpdateDetails update={update} />)).toBe(true);
  });

  it('Should poll an inprogress update in 60 seconds', () => {
    jest.useFakeTimers();
    const apiSpy = createMockApi(update);
    const el = shallow(<Update api={apiSpy} match={{params: params}} />,
      { lifecycleExperimental: true });
    el.setState({update: update});

    expect(setTimeout.mock.calls.length).toBe(1);
    expect(setTimeout.mock.calls[0][1]).toBe(60000);
    expect(apiSpy.getJobUpdateDetails.mock.calls.length).toBe(1);

    jest.runOnlyPendingTimers();
    expect(apiSpy.getJobUpdateDetails.mock.calls.length).toBe(2);
    expect(setTimeout.mock.calls.length).toBe(2);
    expect(setTimeout.mock.calls[1][1]).toBe(60000);
  });

  it('Should not poll when update is not inprogress', () => {
    jest.useFakeTimers();
    const terminatedUpdate = {update: {summary: {state: {status: JobUpdateStatus.ABORTED}}}};
    const apiSpy = createMockApi(terminatedUpdate);
    const el = shallow(<Update api={apiSpy} match={{params: params}} />,
      { lifecycleExperimental: true });
    el.setState({update: terminatedUpdate});

    expect(setTimeout.mock.calls.length).toBe(0);
    expect(apiSpy.getJobUpdateDetails.mock.calls.length).toBe(1);
  });
});
