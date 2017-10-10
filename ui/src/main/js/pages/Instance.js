import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import InstanceHistory from 'components/InstanceHistory';
import Loading from 'components/Loading';
import TaskStatus from 'components/TaskStatus';

import { isActive } from 'utils/Task';

export default class Instance extends React.Component {
  constructor(props) {
    super(props);
    this.state = {cluster: '', tasks: [], loading: true};
  }

  componentWillMount(props) {
    const { role, environment, name, instance } = this.props.match.params;
    const query = new TaskQuery();
    query.role = role;
    query.environment = environment;
    query.jobName = name;
    query.instanceIds = [instance];

    const that = this;
    this.props.api.getTasksWithoutConfigs(query, (rsp) => {
      that.setState({
        cluster: rsp.serverInfo.clusterName,
        loading: false,
        tasks: rsp.result.scheduleStatusResult.tasks
      });
    });
  }

  render() {
    const { role, environment, name, instance } = this.props.match.params;
    if (this.state.loading) {
      return <Loading />;
    }

    const activeTask = this.state.tasks.find(isActive);
    const terminalTasks = this.state.tasks.filter((t) => !isActive(t));
    return (<div className='instance-page'>
      <Breadcrumb
        cluster={this.state.cluster}
        env={environment}
        instance={instance}
        name={name}
        role={role} />
      <TaskStatus task={activeTask} />
      <InstanceHistory tasks={terminalTasks} />
    </div>);
  }
}
