import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import UpdateConfig from 'components/UpdateConfig';
import UpdateDetails from 'components/UpdateDetails';

import { isInProgressUpdate } from 'utils/Update';

export default class Update extends React.Component {
  constructor(props) {
    super(props);
    this.state = { loading: true };
  }

  componentWillMount() {
    this.fetchUpdateDetails();
  }

  componentWillUpdate(nextProps, nextState) {
    if (!nextState.loading && isInProgressUpdate(nextState.update)) {
      // refetch update details in 60 seconds
      setTimeout(() => { this.fetchUpdateDetails(); }, 60000);
    }
  }

  fetchUpdateDetails() {
    const { role, environment, name, uid } = this.props.match.params;

    const job = new JobKey();
    job.role = role;
    job.environment = environment;
    job.name = name;

    const key = new JobUpdateKey();
    key.job = job;
    key.id = uid;

    const query = new JobUpdateQuery();
    query.key = key;

    const that = this;
    this.props.api.getJobUpdateDetails(null, query, (response) => {
      const update = response.result.getJobUpdateDetailsResult.detailsList[0];
      that.setState({ cluster: response.serverInfo.clusterName, loading: false, update });
    });
  }

  render() {
    const { role, environment, name, uid } = this.props.match.params;

    if (this.state.loading) {
      return <Loading />;
    }

    return (<div className='update-page'>
      <Breadcrumb
        cluster={this.state.cluster}
        env={environment}
        name={name}
        role={role}
        update={uid} />
      <UpdateDetails update={this.state.update} />
      <UpdateConfig update={this.state.update} />
    </div>);
  }
}
