import React from 'react';
import { Link } from 'react-router-dom';

import { isNullyOrEmpty } from 'utils/Common';

export function NeighborTaskItem({ assignedTask }) {
  const { role, environment, name } = assignedTask.task.job;
  const taskKey = `${role}/${environment}/${name}/${assignedTask.instanceId}`;
  return (
    <div>
      <span className='task-neighbors-task-key'>
        <Link to={`/beta/scheduler/${taskKey}`}>
          {taskKey}
        </Link>
      </span>
    </div>
  );
}

export default function TaskNeighbors({ tasks }) {
  return (isNullyOrEmpty(tasks) ? <div />
    : (<div className='active-task-neighbors'>
      <h5>host neighbors</h5>
      {tasks.map((t) =>
        <NeighborTaskItem assignedTask={t.assignedTask} key={t.assignedTask.taskId} />)}
    </div>));
}
