import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';
import UpdateList from 'components/UpdateList';

import { getInProgressStates, getTerminalStates } from 'utils/Update';

export const MAX_QUERY_SIZE = 100;

export class UpdatesFetcher extends React.Component {
  constructor(props) {
    super(props);
    this.state = { updates: null };
  }

  componentWillMount() {
    const that = this;
    const query = new JobUpdateQuery();
    query.updateStatuses = this.props.states;
    query.limit = MAX_QUERY_SIZE;
    this.props.api.getJobUpdateSummaries(query, (response) => {
      const updates = response.result.getJobUpdateSummariesResult.updateSummaries;
      that.setState({updates});
      that.props.clusterFn(response.serverInfo.clusterName);
    });
  }

  render() {
    return (<Container>
      <PanelGroup noPadding title={<StandardPanelTitle title={this.props.title} />}>
        <UpdateList updates={this.state.updates} />
      </PanelGroup>
    </Container>);
  }
}

export default class Updates extends React.Component {
  constructor(props) {
    super(props);
    this.state = { cluster: null };
    this.clusterFn = this.setCluster.bind(this);
  }

  setCluster(cluster) {
    // TODO(dmcg): We should just have the Scheduler return the cluster as a global.
    this.setState({cluster});
  }

  render() {
    const api = this.props.api;
    return (<div className='update-page'>
      <Breadcrumb cluster={this.state.cluster} />
      <UpdatesFetcher
        api={api}
        clusterFn={this.clusterFn}
        states={getInProgressStates()}
        title='Updates In Progress' />
      <UpdatesFetcher
        api={api}
        clusterFn={this.clusterFn}
        states={getTerminalStates()}
        title='Recently Completed Updates' />
    </div>);
  }
}
