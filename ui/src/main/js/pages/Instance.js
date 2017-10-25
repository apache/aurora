import React from 'react';
import deepEqual from 'deep-equal';

import Breadcrumb from 'components/Breadcrumb';
import InstanceHistory from 'components/InstanceHistory';
import Loading from 'components/Loading';
import TaskStatus from 'components/TaskStatus';

import { isActive } from 'utils/Task';

export default class Instance extends React.Component {
  getInitialState() {
    return {cluster: '', tasks: [], loading: true};
  }

  constructor(props) {
    super(props);
    this.state = this.getInitialState();
  }

  fetchTask(role, environment, name, instance) {
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

  componentWillMount(props) {
    const { role, environment, name, instance } = this.props.match.params;
    this.fetchTask(role, environment, name, instance);
  }

  componentWillUpdate(nextProps, nextState) {
    if (this.state.loading && !nextState.loading) {
      const activeTask = nextState.tasks.find(isActive);

      const query = new TaskQuery();
      query.statuses = [ScheduleStatus.RUNNING];
      query.slaveHosts = [activeTask.assignedTask.slaveHost];

      const that = this;
      this.props.api.getTasksWithoutConfigs(query, (rsp) => {
        const tasksOnAgent = rsp.result.scheduleStatusResult.tasks;
        that.setState({
          neighborTasks: tasksOnAgent.filter(function (el) {
            return el.assignedTask.taskId !== activeTask.assignedTask.taskId;
          })
        });
      });
    }

    if (!deepEqual(this.props.match.params, nextProps.match.params)) {
      const { role, environment, name, instance } = nextProps.match.params;
      this.setState(this.getInitialState());
      this.fetchTask(role, environment, name, instance);
    }
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
      <TaskStatus neighbors={this.state.neighborTasks} task={activeTask} />
      <InstanceHistory tasks={terminalTasks} />
    </div>);
  }
}
