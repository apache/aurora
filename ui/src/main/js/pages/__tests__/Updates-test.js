import React from 'react';
import { shallow } from 'enzyme';

import { UpdatesFetcher } from '../Updates';
import UpdateList from 'components/UpdateList';

const TEST_CLUSTER = 'test-cluster';

function createMockApi(updates) {
  const api = {};
  api.getJobUpdateSummaries = (query, handler) => handler({
    result: {
      getJobUpdateSummariesResult: {
        updateSummaries: updates
      }
    },
    serverInfo: {
      clusterName: TEST_CLUSTER
    }
  });
  return api;
}

const updates = [{}]; // only testing pass-through here...
const states = [];

describe('UpdatesFetcher', () => {
  it('Should render update list with updates when mounted', () => {
    const el = shallow(
      <UpdatesFetcher api={createMockApi(updates)} clusterFn={() => {}} states={states} />);
    expect(el.contains(<UpdateList updates={updates} />));
  });
});
