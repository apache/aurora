import React from 'react';

import Icon from 'components/Icon';
import Pagination from 'components/Pagination';
import TaskListItem from 'components/TaskListItem';

import { isNully, pluralize } from 'utils/Common';
import { getClassForScheduleStatus, getLastEventTime } from 'utils/Task';
import { SCHEDULE_STATUS } from 'utils/Thrift';

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

    const reasons = isNully(this.props.pendingReasons) ? {} : this.props.pendingReasons;
    this.props.tasks.forEach((t) => {
      if (t.status === ScheduleStatus.PENDING && reasons[t.assignedTask.taskId]) {
        t.taskEvents[t.taskEvents.length - 1].message = reasons[t.assignedTask.taskId];
      }
    });

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
