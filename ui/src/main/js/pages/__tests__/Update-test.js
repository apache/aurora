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
  api.getJobUpdateDetails = (id, query, handler) => handler({
    result: {
      getJobUpdateDetailsResult: {
        detailsList: [update]
      }
    },
    serverInfo: {
      clusterName: TEST_CLUSTER
    }
  });
  return api;
}

const update = {}; // only testing pass-through here...

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
});
