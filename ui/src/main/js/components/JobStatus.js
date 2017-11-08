import React from 'react';

import JobConfig from 'components/JobConfig';
import PanelGroup from 'components/Layout';
import Tabs, { Tab } from 'components/Tabs';
import TaskList from 'components/TaskList';

import { isNully, sort } from 'utils/Common';
import { isActive } from 'utils/Task';

export default function ({
  configGroups,
  cronJob,
  onTaskViewChange,
  pendingReasons,
  queryParams,
  tasks }) {
  const activeTasks = sort(tasks.filter(isActive), (t) => t.assignedTask.instanceId);
  const numberConfigs = isNully(cronJob) ? (isNully(configGroups) ? '' : configGroups.length) : 1;
  return (<Tab id='status' name={`Active Tasks (${activeTasks.length})`}>
    <PanelGroup>
      <Tabs
        activeTab={queryParams.taskView}
        className='task-status-tabs'
        onChange={onTaskViewChange}>
        <Tab icon='th-list' id='tasks' name='Tasks'>
          <TaskList pendingReasons={pendingReasons} tasks={activeTasks} />
        </Tab>
        <Tab icon='info-sign' id='config' name={`Configuration (${numberConfigs})`}>
          <JobConfig cronJob={cronJob} groups={configGroups} />
        </Tab>
      </Tabs>
    </PanelGroup>
  </Tab>);
}
