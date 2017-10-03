import React from 'react';
import { shallow } from 'enzyme';

import Jobs from '../Jobs';
import Breadcrumb from 'components/Breadcrumb';
import JobList from 'components/JobList';
import Loading from 'components/Loading';
import RoleQuota from 'components/RoleQuota';

const TEST_CLUSTER = 'test-cluster';
const TEST_ENV = 'test-env';
const TEST_ROLE = 'test-role';

function createMockApi(jobs, quota) {
  const api = {};
  api.getJobSummary = (role, handler) => handler({
    result: {
      jobSummaryResult: {
        summaries: jobs
      }
    },
    serverInfo: {
      clusterName: TEST_CLUSTER
    }
  });

  api.getQuota = (role, handler) => handler({
    result: {
      getQuotaResult: quota
    },
    serverInfo: {
      clusterName: TEST_CLUSTER
    }
  });

  return api;
}

const jobs = [{job: {key: {environment: TEST_ENV}}}, {job: {key: {environment: 'wrong-env'}}}];
const quota = {}; // No keys are accessed, so just do reference equality later.

describe('Jobs', () => {
  it('Should render Loading before data is fetched', () => {
    expect(shallow(<Jobs
      api={{getJobSummary: () => {}, getQuota: () => {}}}
      match={{params: {role: TEST_ROLE}}} />).equals(<Loading />)).toBe(true);
  });

  it('Should render page elements when jobs are fetched', () => {
    const el = shallow(
      <Jobs api={createMockApi(jobs, quota)} match={{params: {role: TEST_ROLE}}} />);
    expect(el.contains(
      <Breadcrumb cluster={TEST_CLUSTER} env={undefined} role={TEST_ROLE} />)).toBe(true);
    expect(el.find(JobList).length).toBe(1);
    expect(el.find(RoleQuota).length).toBe(1);
  });

  it('Should pass through environment path parameter and filter jobs', () => {
    const home = shallow(<Jobs
      api={createMockApi(jobs)}
      match={{params: {environment: TEST_ENV, role: TEST_ROLE}}} />);
    expect(home.contains(
      <Breadcrumb cluster={TEST_CLUSTER} env={TEST_ENV} role={TEST_ROLE} />)).toBe(true);
  });
});
