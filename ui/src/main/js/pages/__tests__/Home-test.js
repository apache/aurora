import React from 'react';
import { shallow } from 'enzyme';

import Home from '../Home';
import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import RoleList from 'components/RoleList';

const TEST_CLUSTER = 'test-cluster';

function createMockApi(roles) {
  const api = {};
  api.getRoleSummary = (handler) => handler({
    result: {
      roleSummaryResult: {
        summaries: roles
      }
    },
    serverInfo: {
      clusterName: TEST_CLUSTER
    }
  });
  return api;
}

const roles = [{role: 'test', jobCount: 0, cronJobCount: 5}];

describe('Home', () => {
  it('Should render Loading before data is fetched', () => {
    expect(shallow(<Home api={{getRoleSummary: () => {}}} />).contains(<Loading />)).toBe(true);
  });

  it('Should render page elements when roles are fetched', () => {
    const home = shallow(<Home api={createMockApi(roles)} />);
    expect(home.contains(<Breadcrumb cluster={TEST_CLUSTER} />)).toBe(true);
    expect(home.contains(<RoleList roles={roles} />)).toBe(true);
  });
});
