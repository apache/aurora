import React from 'react';
import queryString from 'query-string';

import Breadcrumb from 'components/Breadcrumb';
import JobOverview from 'components/JobOverview';
import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';
import { JobUpdateList } from 'components/UpdateList';
import UpdatePreview from 'components/UpdatePreview';

import { isNully } from 'utils/Common';
import { isInProgressUpdate } from 'utils/Update';

export default class Job extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      cluster: props.cluster || '',
      configGroups: props.configGroups,
      tasks: props.tasks,
      updates: props.updates,
      pendingReasons: props.pendingReasons,
      cronJob: props.cronJob
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
      if (response.result.getPendingReasonResult.reasons) {
        const reasons = response.result.getPendingReasonResult.reasons.reduce((res, reason) => {
          res[reason.taskId] = reason.reason;
          return res;
        }, {});
        that.setState({
          cluster: response.serverInfo.clusterName,
          pendingReasons: reasons
        });
      }
    });
    api.getConfigSummary(key, (response) => {
      that.setState({
        cluster: response.serverInfo.clusterName,
        configGroups: response.result.configSummaryResult.summary.groups
      });
    });
    api.getJobSummary(role, (response) => {
      const cronJob = response.result.jobSummaryResult.summaries.find((j) => {
        return j.job.key.environment === that.props.match.params.environment &&
          j.job.key.name === that.props.match.params.name &&
          !isNully(j.job.cronSchedule);
      });

      if (cronJob) {
        that.setState({
          cluster: response.serverInfo.clusterName,
          cronJob
        });
      }
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
      .filter((u) => !isInProgressUpdate(u));

    if (terminalUpdates.length === 0) {
      return '';
    }

    return (<Container>
      <PanelGroup noPadding title={<StandardPanelTitle title='Update History' />}>
        <JobUpdateList updates={terminalUpdates} />
      </PanelGroup>
    </Container>);
  }

  setJobView(tabId) {
    const {match: {params: {role, environment, name}}} = this.props;
    this.props.history.push(`/scheduler/${role}/${environment}/${name}?jobView=${tabId}`);
  }

  setTaskView(tabId) {
    const {match: {params: {role, environment, name}}} = this.props;
    this.props.history.push(`/scheduler/${role}/${environment}/${name}?taskView=${tabId}`);
  }

  render() {
    const that = this;
    return (<div className='job-page'>
      <Breadcrumb
        cluster={this.state.cluster}
        env={this.props.match.params.environment}
        name={this.props.match.params.name}
        role={this.props.match.params.role} />
      {this.updateInProgress()}
      <Container>
        <JobOverview
          configGroups={this.state.configGroups}
          cronJob={this.state.cronJob}
          onTaskViewChange={(t) => that.setTaskView(t.id)}
          onViewChange={(t) => that.setJobView(t.id)}
          pendingReasons={this.state.pendingReasons}
          queryParams={queryString.parse(this.props.location.search)}
          tasks={this.state.tasks} />
      </Container>
      {this.updateHistory()}
    </div>);
  }
}
