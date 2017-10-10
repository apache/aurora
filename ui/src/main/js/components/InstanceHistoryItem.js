import moment from 'moment';
import React from 'react';

import Icon from 'components/Icon';
import StateMachine from 'components/StateMachine';

import {
  getClassForScheduleStatus,
  getDuration,
  taskToStateMachine
} from 'utils/Task';
import { SCHEDULE_STATUS } from 'utils/Thrift';

export function InstanceHistoryBody({ task }) {
  const states = taskToStateMachine(task);
  return [
    <div className='instance-history-item-body'>
      <StateMachine
        className={getClassForScheduleStatus(task.status)}
        states={states} />
    </div>,
    <div className='instance-history-item-footer'>
      <span><strong>Task ID</strong> {task.assignedTask.taskId}</span>
    </div>
  ];
}

export function InstanceHistoryHeader({ task, toggle }) {
  const latestEvent = task.taskEvents[task.taskEvents.length - 1];
  return (<div className='instance-history-item'>
    <span className={`img-circle ${getClassForScheduleStatus(task.status)}`} />
    <div className='instance-history-item-details' onClick={toggle}>
      <div className='instance-history-status'>
        <h5>{SCHEDULE_STATUS[task.status]}</h5>
        <span className='instance-history-time'>
          <span>{moment(latestEvent.timestamp).fromNow()}</span>
          <span> &bull; </span>
          <span>Running duration: {getDuration(task)}</span>
        </span>
      </div>
      <div>
        <span className='instance-history-message'>{latestEvent.message}</span>
      </div>
    </div>
    <ul className='instance-history-item-actions'>
      <li><a href={`http://${task.assignedTask.slaveHost}:1338/task/${task.assignedTask.taskId}`}>
        {task.assignedTask.slaveHost}
      </a></li>
      <li>
        <a
          className='tip'
          data-tip='View task config'
          href={`/structdump/task/${task.assignedTask.taskId}`}>
          <Icon name='info-sign' />
        </a>
      </li>
    </ul>
  </div>);
}

export default class InstanceHistoryItem extends React.Component {
  constructor(props) {
    super(props);
    this.state = {expanded: props.expanded || false};
    this._toggle = this.toggle.bind(this);
  }

  toggle() {
    this.setState({expanded: !this.state.expanded});
  }

  render() {
    const body = this.state.expanded ? <InstanceHistoryBody task={this.props.task} /> : '';
    return <div><InstanceHistoryHeader task={this.props.task} toggle={this._toggle} />{body}</div>;
  }
}
