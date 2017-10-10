import React from 'react';

import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';
import StateMachine from 'components/StateMachine';
import TaskDetails from 'components/TaskDetails';

import { isNully } from 'utils/Common';
import { getClassForScheduleStatus, taskToStateMachine } from 'utils/Task';

export default function TaskStatus({ task }) {
  if (isNully(task)) {
    return (<Container>
      <PanelGroup title={<StandardPanelTitle title='Active Task' />}>
        <div>No active task found.</div>
      </PanelGroup>
    </Container>);
  }

  return (<Container>
    <PanelGroup title={<StandardPanelTitle title='Active Task' />}>
      <div className='row'>
        <div className='col-md-6'>
          <TaskDetails task={task} />
        </div>
        <div className='col-md-6'>
          <StateMachine
            className={getClassForScheduleStatus(task.status)}
            states={taskToStateMachine(task)} />
        </div>
      </div>
    </PanelGroup>
  </Container>);
}
