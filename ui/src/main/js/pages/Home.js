import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import RoleList from 'components/RoleList';

export default class HomePage extends React.Component {
  constructor(props) {
    super(props);
    this.state = {cluster: '', roles: [], loading: true};
  }

  componentWillMount(props) {
    const that = this;
    this.props.api.getRoleSummary((response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        loading: false,
        roles: response.result.roleSummaryResult.summaries
      });
    });
  }

  render() {
    return this.state.loading ? <Loading /> : (<div>
      <Breadcrumb cluster={this.state.cluster} />
      <div className='container'>
        <div className='row'>
          <div className='col-md-12'>
            <div className='panel'>
              <RoleList roles={this.state.roles} />
            </div>
          </div>
        </div>
      </div>
    </div>);
  }
}
