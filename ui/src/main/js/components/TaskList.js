import React from 'react';
import { Link } from 'react-router-dom';

import Icon from 'components/Icon';
import Pagination from 'components/Pagination';
import { RelativeTime } from 'components/Time';
import TaskStateMachine from 'components/TaskStateMachine';

import { pluralize } from 'utils/Common';
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

// VisibleForTesting
export function searchTask(task, userQuery) {
  const query = userQuery.toLowerCase();
  return (task.assignedTask.instanceId.toString().startsWith(query) ||
    (task.assignedTask.slaveHost && task.assignedTask.slaveHost.toLowerCase().includes(query)) ||
    SCHEDULE_STATUS[task.status].toLowerCase().startsWith(query));
}

export function TaskListFilter({ numberPerPage, onChange, tasks }) {
  if (tasks.length > numberPerPage) {
    return (<div className='table-input-wrapper'>
      <Icon name='search' />
      <input
        autoFocus
        onChange={(e) => onChange(e)}
        placeholder='Search tasks by instance-id, host or current status'
        type='text' />
    </div>);
  }
  return null;
}

export function TaskListStatus({ status }) {
  return [
    <span className={`img-circle ${getClassForScheduleStatus(ScheduleStatus[status])}`} />,
    <span>{status}</span>
  ];
}

export function TaskListStatusFilter({ onClick, tasks }) {
  const statuses = Object.keys(tasks.reduce((seen, task) => {
    seen[SCHEDULE_STATUS[task.status]] = true;
    return seen;
  }, {}));

  if (statuses.length <= 1) {
    return (<div>
      {pluralize(tasks, 'One task is ', `All ${tasks.length} tasks are `)}
      <TaskListStatus status={statuses[0]} />
    </div>);
  }

  return (<ul className='task-list-status-filter'>
    <li>Filter by:</li>
    <li onClick={(e) => onClick(null)}>all</li>
    {statuses.map((status) => (<li key={status} onClick={(e) => onClick(status)}>
      <TaskListStatus status={status} />
    </li>))}
  </ul>);
}

export function TaskListControls({ currentSort, onFilter, onSort, tasks }) {
  return (<div className='task-list-controls'>
    <ul className='task-list-main-sort'>
      <li>Sort by:</li>
      <li className={currentSort === 'default' ? 'active' : ''} onClick={(e) => onSort('default')}>
        instance
      </li>
      <li className={currentSort === 'latest' ? 'active' : ''} onClick={(e) => onSort('latest')}>
        updated
      </li>
    </ul>
    <TaskListStatusFilter onClick={onFilter} tasks={tasks} />
  </div>);
}

export default class TaskList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      filter: props.filter,
      reverseSort: props.reverse || false,
      sortBy: props.sortBy || 'default'
    };
  }

  setFilter(filter) {
    this.setState({filter});
  }

  setSort(sortBy) {
    if (sortBy === this.state.sortBy) {
      this.setState({reverseSort: !this.state.reverseSort});
    } else {
      this.setState({sortBy});
    }
  }

  render() {
    const that = this;
    const tasksPerPage = 25;
    const filterFn = (t) => that.state.filter ? searchTask(t, that.state.filter) : true;
    const sortFn = this.state.sortBy === 'latest'
      ? (t) => getLastEventTime(t) * -1
      : (t) => t.assignedTask.instanceId;

    return (<div>
      <TaskListFilter
        numberPerPage={tasksPerPage}
        onChange={(e) => that.setFilter(e.target.value)}
        tasks={this.props.tasks} />
      <TaskListControls
        currentSort={this.state.sortBy}
        onFilter={(query) => that.setFilter(query)}
        onSort={(key) => that.setSort(key)}
        tasks={this.props.tasks} />
      <div className='task-list'>
        <table className='psuedo-table'>
          <Pagination
            data={this.props.tasks}
            filter={filterFn}
            hideIfSinglePage
            isTable
            numberPerPage={tasksPerPage}
            renderer={(t) => <TaskListItem key={t.assignedTask.taskId} task={t} />}
            reverseSort={this.state.reverseSort}
            sortBy={sortFn} />
        </table>
      </div>
    </div>);
  }
}
