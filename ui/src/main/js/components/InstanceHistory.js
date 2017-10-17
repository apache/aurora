import React from 'react';

import InstanceHistoryItem from 'components/InstanceHistoryItem';
import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';

import { isNully } from 'utils/Common';
import { getLastEventTime } from 'utils/Task';

export default function InstanceHistory({ tasks }) {
  if (isNully(tasks) || tasks.length === 0) {
    return <div />;
  }

  const sortedTasks = tasks.sort((a, b) => {
    return getLastEventTime(a) > getLastEventTime(b) ? -1 : 1;
  });
  return (<Container className='instance-history'>
    <PanelGroup noPadding title={<StandardPanelTitle title='Instance History' />}>
      {sortedTasks.map((t) => <InstanceHistoryItem key={t.assignedTask.taskId} task={t} />)}
    </PanelGroup>
  </Container>);
}
