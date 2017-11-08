import React from 'react';

export default function TaskDetails({ task }) {
  return (<div className='active-task-details'>
    <div>
      <h5>Task ID</h5>
      <span className='debug-data'>{task.assignedTask.taskId}</span>
      <a href={`/structdump/task/${task.assignedTask.taskId}`}>view raw config</a>
    </div>
    {task.assignedTask.slaveHost ? <div className='active-task-details-host'>
      <h5>Host</h5>
      <span className='debug-data'>{task.assignedTask.slaveHost}</span>
      <a href={`http://${task.assignedTask.slaveHost}:1338/task/${task.assignedTask.taskId}`}>
        view sandbox
      </a>
    </div> : null}
  </div>);
}
