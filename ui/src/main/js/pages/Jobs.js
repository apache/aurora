import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import JobList from 'components/JobList';
import PanelGroup, { PanelSubtitle, StandardPanelTitle } from 'components/Layout';
import Loading from 'components/Loading';
import RoleQuota from 'components/RoleQuota';

import { isNully } from 'utils/Common';

export default class Jobs extends React.Component {
  constructor(props) {
    super(props);
    this.state = {cluster: '', jobs: [], loading: isNully(props.jobs)};
  }

  componentWillMount() {
    const that = this;
    this.props.api.getJobSummary(this.props.match.params.role, (response) => {
      const jobs = (that.props.match.params.environment)
        ? response.result.jobSummaryResult.summaries.filter(
          (j) => j.job.key.environment === that.props.match.params.environment)
        : response.result.jobSummaryResult.summaries;
      that.setState({ cluster: response.serverInfo.clusterName, loading: false, jobs });
    });

    this.props.api.getQuota(this.props.match.params.role, (response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        loading: false,
        quota: response.result.getQuotaResult
      });
    });
  }

  render() {
    return this.state.loading ? <Loading /> : (<div>
      <Breadcrumb
        cluster={this.state.cluster}
        env={this.props.match.params.environment}
        role={this.props.match.params.role} />
      <div className='container'>
        <div className='row'>
          <div className='col-md-6'>
            <PanelGroup noPadding title={<PanelSubtitle title='Resources' />}>
              <RoleQuota quota={this.state.quota} />
            </PanelGroup>
          </div>
        </div>
        <div className='row'>
          <div className='col-md-12'>
            <PanelGroup title={<StandardPanelTitle title='Jobs' />}>
              <JobList env={this.props.match.params.environment} jobs={this.state.jobs} />
            </PanelGroup>
          </div>
        </div>
      </div>
    </div>);
  }
}
