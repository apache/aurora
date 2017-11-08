import React from 'react';

import PanelGroup from 'components/Layout';
import { Tab } from 'components/Tabs';
import TaskList from 'components/TaskList';

import { sort } from 'utils/Common';
import { getLastEventTime, isActive } from 'utils/Task';

export default function ({ pendingReasons, tasks }) {
  const terminalTasks = sort(tasks.filter((t) => !isActive(t)), (t) => getLastEventTime(t), true);
  return (<Tab id='history' name={`Completed Tasks (${terminalTasks.length})`}>
    <PanelGroup>
      <TaskList pendingReasons={pendingReasons} sortBy='latest' tasks={terminalTasks} />
    </PanelGroup>
  </Tab>);
}
