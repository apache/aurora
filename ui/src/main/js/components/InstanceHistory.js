import React from 'react';

import InstanceHistoryItem from 'components/InstanceHistoryItem';
import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';

import { getLastEventTime } from 'utils/Task';

export default function InstanceHistory({ tasks }) {
  const sortedTasks = tasks.sort((a, b) => {
    return getLastEventTime(a) > getLastEventTime(b) ? -1 : 1;
  });

  return (<Container className='instance-history'>
    <PanelGroup noPadding title={<StandardPanelTitle title='Instance History' />}>
      {sortedTasks.length > 0
        ? sortedTasks.map((t) => <InstanceHistoryItem key={t.assignedTask.taskId} task={t} />)
        : <div>No task history found.</div>}
    </PanelGroup>
  </Container>);
}
