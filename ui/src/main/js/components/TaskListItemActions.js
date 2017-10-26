import React from 'react';

export default function ({ task }) {
  return (<div className='task-list-item-host'>
    <a href={`http://${task.assignedTask.slaveHost}:1338/task/${task.assignedTask.taskId}`}>
      {task.assignedTask.slaveHost}
    </a>
  </div>);
}
