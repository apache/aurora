import React from 'react';
import { Link } from 'react-router-dom';

import Pagination from 'components/Pagination';
import { RelativeTime } from 'components/Time';
import TaskStateMachine from 'components/TaskStateMachine';

import { getClassForScheduleStatus, getDuration, getLastEventTime, isActive } from 'utils/Task';
import { SCHEDULE_STATUS } from 'utils/Thrift';

export class TaskListItem extends React.Component {
  constructor(props) {
    super(props);
    this.state = {expand: props.expand || false};
  }

  toggleExpand() {
    this.setState({expanded: !this.state.expanded});
  }

  render() {
    const task = this.props.task;
    const { role, environment, name } = task.assignedTask.task.job;
    const latestEvent = task.taskEvents[task.taskEvents.length - 1];
    const active = isActive(task);
    const stateMachine = (this.state.expanded) ? <TaskStateMachine task={task} /> : '';
    return (<tr className={this.state.expanded ? 'expanded' : ''}>
      <td>
        <div className='task-list-item-instance'>
          <Link
            to={`/beta/scheduler/${role}/${environment}/${name}/${task.assignedTask.instanceId}`}>
            {task.assignedTask.instanceId}
          </Link>
        </div>
      </td>
      <td className='task-list-item-col'>
        <div className='task-list-item'>
          <span className='task-list-item-status'>
            {SCHEDULE_STATUS[task.status]}
          </span>
          <span className={`img-circle ${getClassForScheduleStatus(task.status)}`} />
          <span className='task-list-item-time'>
            {active ? 'since' : ''} <RelativeTime ts={getLastEventTime(task)} />
          </span>
          {active ? ''
            : <span className='task-list-item-duration'>(ran for {getDuration(task)})</span>}
          <span className='task-list-item-expander' onClick={(e) => this.toggleExpand()}>
            ...
          </span>
          <span className='task-list-item-message'>
            {latestEvent.message}
          </span>
        </div>
        {stateMachine}
      </td>
      <td>
        <div className='task-list-item-host'>
          <a href={`http://${task.assignedTask.slaveHost}:1338/task/${task.assignedTask.taskId}`}>
            {task.assignedTask.slaveHost}
          </a>
        </div>
      </td>
    </tr>);
  }
}

export default function TaskList({ tasks }) {
  return (<div className='task-list'>
    <table className='psuedo-table'>
      <Pagination
        data={tasks}
        hideIfSinglePage
        isTable
        numberPerPage={25}
        renderer={(t) => <TaskListItem key={t.assignedTask.taskId} task={t} />} />
    </table>
  </div>);
}
