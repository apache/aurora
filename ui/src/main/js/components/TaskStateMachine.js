import React from 'react';

import StateMachine from 'components/StateMachine';

import { getClassForScheduleStatus, taskToStateMachine } from 'utils/Task';

export default function TaskStateMachine({ task }) {
  const states = taskToStateMachine(task);
  return <StateMachine className={getClassForScheduleStatus(task.status)} states={states} />;
}
