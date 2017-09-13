import React from 'react';

import Home from '../Home';
import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import RoleList from 'components/RoleList';
import shallow from 'utils/ShallowRender';

import chai, { expect } from 'chai';
import assertJsx from 'preact-jsx-chai';
chai.use(assertJsx);

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
    expect(<Home api={{getRoleSummary: () => {}}} />).to.deep.equal(<Loading />);
  });

  it('Should render page elements when roles are fetched', () => {
    const home = shallow(<Home api={createMockApi(roles)} />);
    expect(home.contains(<Breadcrumb cluster={TEST_CLUSTER} />)).to.be.true;
    expect(home.contains(<RoleList roles={roles} />)).to.be.true;
  });
});
