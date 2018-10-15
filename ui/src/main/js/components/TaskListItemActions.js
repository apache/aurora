import React from 'react';

import { isThermos } from 'utils/Task';

export default function ({ task }) {
  if (isThermos(task.assignedTask.task)) {
    return (<div className='task-list-item-host'>
      <a href={`http://${task.assignedTask.slaveHost}:1338/task/${task.assignedTask.taskId}`}>
        {task.assignedTask.slaveHost}
      </a>
    </div>);
  } else {
    return (<div className='task-list-item-host'>
      <strong>{task.assignedTask.slaveHost}</strong>
    </div>);
  }
}
