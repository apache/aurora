import React from 'react';

import Breadcrumb from 'components/Breadcrumb';
import JobConfig from 'components/JobConfig';
import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';
import Loading from 'components/Loading';
import Tabs from 'components/Tabs';
import TaskList from 'components/TaskList';
import { JobUpdateList } from 'components/UpdateList';
import UpdatePreview from 'components/UpdatePreview';

import { isNully, sort } from 'utils/Common';
import { getLastEventTime, isActive } from 'utils/Task';
import { isInProgressUpdate } from 'utils/Update';

export default class Job extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      cluster: props.cluster || '',
      configGroups: props.configGroups,
      tasks: props.tasks,
      updates: props.updates,
      pendingReasons: props.pendingReasons
    };
  }

  componentWillMount() {
    const {api, match: {params: {role, environment, name}}} = this.props;
    const that = this;
    const key = new JobKey({role, environment, name});

    const taskQuery = new TaskQuery();
    taskQuery.role = role;
    taskQuery.environment = environment;
    taskQuery.jobName = name;
    api.getTasksWithoutConfigs(taskQuery, (response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        tasks: response.result.scheduleStatusResult.tasks
      });
    });
    api.getPendingReason(taskQuery, (response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        pendingReasons: response.result.getPendingReasonResult.reasons
      });
    });
    api.getConfigSummary(key, (response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        configGroups: response.result.configSummaryResult.summary.groups
      });
    });

    const updateQuery = new JobUpdateQuery();
    updateQuery.jobKey = key;
    api.getJobUpdateDetails(null, updateQuery, (response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        updates: response.result.getJobUpdateDetailsResult.detailsList
      });
    });
  }

  updateInProgress() {
    if (!this.state.updates) {
      return '';
    }

    const updateInProgress = this.state.updates.find(isInProgressUpdate);
    if (!updateInProgress) {
      return '';
    }
    return <UpdatePreview update={updateInProgress} />;
  }

  updateHistory() {
    if (!this.state.updates || this.state.updates.length === 0) {
      return '';
    }

    const terminalUpdates = this.state.updates
      .filter((u) => !isInProgressUpdate(u))
      .map((u) => u.update.summary);

    if (terminalUpdates.length === 0) {
      return '';
    }

    return (<Container>
      <PanelGroup noPadding title={<StandardPanelTitle title='Update History' />}>
        <JobUpdateList updates={terminalUpdates} />
      </PanelGroup>
    </Container>);
  }

  jobHistoryTab() {
    const terminalTasks = sort(
      this.state.tasks.filter((t) => !isActive(t)), (t) => getLastEventTime(t), true);

    return {
      name: `Job History (${terminalTasks.length})`,
      content: <PanelGroup><TaskList tasks={terminalTasks} /></PanelGroup>
    };
  }

  jobStatusTab() {
    const activeTasks = sort(this.state.tasks.filter(isActive), (t) => t.assignedTask.instanceId);
    const numberConfigs = isNully(this.state.configGroups) ? '' : this.state.configGroups.length;
    return {
      name: 'Job Status',
      content: (<PanelGroup>
        <Tabs className='task-status-tabs' tabs={[
          {icon: 'th-list', name: 'Tasks', content: <TaskList tasks={activeTasks} />},
          {
            icon: 'info-sign',
            name: `Configuration (${numberConfigs})`,
            content: <JobConfig groups={this.state.configGroups} />
          }]} />
      </PanelGroup>)
    };
  }

  jobOverview() {
    if (isNully(this.state.tasks)) {
      return <Loading />;
    }
    return <Tabs className='job-overview' tabs={[this.jobStatusTab(), this.jobHistoryTab()]} />;
  }

  render() {
    return (<div className='job-page'>
      <Breadcrumb
        cluster={this.state.cluster}
        env={this.props.match.params.environment}
        name={this.props.match.params.name}
        role={this.props.match.params.role} />
      {this.updateInProgress()}
      <Container>
        {this.jobOverview()}
      </Container>
      {this.updateHistory()}
    </div>);
  }
}
