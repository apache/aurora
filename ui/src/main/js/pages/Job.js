import React from 'react';
import queryString from 'query-string';

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

export const TASK_CONFIG_TAB = 'config';
export const TASK_LIST_TAB = 'tasks';
export const JOB_STATUS_TAB = 'status';
export const JOB_HISTORY_TAB = 'history';

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
      id: JOB_HISTORY_TAB,
      name: `Job History (${terminalTasks.length})`,
      content: <PanelGroup><TaskList tasks={terminalTasks} /></PanelGroup>
    };
  }

  setJobView(tabId) {
    const {match: {params: {role, environment, name}}} = this.props;
    this.props.history.push(`/beta/scheduler/${role}/${environment}/${name}?jobView=${tabId}`);
  }

  setTaskView(tabId) {
    const {match: {params: {role, environment, name}}} = this.props;
    this.props.history.push(`/beta/scheduler/${role}/${environment}/${name}?taskView=${tabId}`);
  }

  jobStatusTab() {
    const activeTasks = sort(this.state.tasks.filter(isActive), (t) => t.assignedTask.instanceId);
    const numberConfigs = isNully(this.state.configGroups) ? '' : this.state.configGroups.length;
    return {
      id: JOB_STATUS_TAB,
      name: 'Job Status',
      content: (<PanelGroup>
        <Tabs
          activeTab={queryString.parse(this.props.location.search).taskView}
          className='task-status-tabs'
          onChange={(t) => this.setTaskView(t.id)}
          tabs={[
            {
              icon: 'th-list',
              id: TASK_LIST_TAB,
              name: 'Tasks',
              content: <TaskList tasks={activeTasks} />
            },
            {
              icon: 'info-sign',
              id: TASK_CONFIG_TAB,
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
    return <Tabs
      activeTab={queryString.parse(this.props.location.search).jobView}
      className='job-overview'
      onChange={(t) => this.setJobView(t.id)}
      tabs={[this.jobStatusTab(), this.jobHistoryTab()]} />;
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
