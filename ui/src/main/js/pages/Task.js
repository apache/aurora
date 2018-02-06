import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import Loading from 'components/Loading';
import TaskStatus from 'components/TaskStatus';

import { isNullyOrEmpty } from 'utils/Common';

export default class Task extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      cluster: props.cluster || '',
      loading: isNullyOrEmpty(props.task),
      task: props.task
    };
  }

  componentWillMount() {
    const {api, match: {params: {role, environment, name, taskId}}} = this.props;
    const that = this;

    const taskQuery = new TaskQuery();
    taskQuery.role = role;
    taskQuery.environment = environment;
    taskQuery.jobName = name;
    taskQuery.taskIds = [taskId];
    api.getTasksWithoutConfigs(taskQuery, (response) => {
      const tasks = response.result.scheduleStatusResult.tasks;
      if (!isNullyOrEmpty(tasks)) {
        that.setState({
          cluster: response.serverInfo.clusterName,
          loading: false,
          task: response.result.scheduleStatusResult.tasks[0]
        });
      } else {
        that.setState({
          cluster: response.serverInfo.clusterName,
          loading: false
        });
      }
    });
  }

  render() {
    if (this.state.loading) {
      return <Loading />;
    } else if (isNullyOrEmpty(this.state.task)) {
      return <div>Task not found, it may have been automatically pruned.</div>;
    }

    return (<div className='task-page'>
      <Breadcrumb
        cluster={this.state.cluster}
        env={this.props.match.params.environment}
        name={this.props.match.params.name}
        role={this.props.match.params.role}
        taskId={this.props.match.params.taskId} />

      <TaskStatus task={this.state.task} title={`Task: ${this.props.match.params.taskId}`} />
    </div>);
  }
}
