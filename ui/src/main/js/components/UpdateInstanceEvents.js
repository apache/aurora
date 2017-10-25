import moment from 'moment';
import React from 'react';
import { Link } from 'react-router-dom';

import Icon from 'components/Icon';
import Pagination from 'components/Pagination';
import StateMachine from 'components/StateMachine';

import { addClass, sort } from 'utils/Common';
import { UPDATE_ACTION } from 'utils/Thrift';
import { actionDispatcher, getClassForUpdateAction } from 'utils/Update';

const instanceEventIcon = actionDispatcher({
  success: (e) => <Icon name='ok' />,
  warning: (e) => <Icon name='warning-sign' />,
  error: (e) => <Icon name='remove' />,
  inProgress: (e) => <Icon name='play-circle' />
});

export class InstanceEvent extends React.Component {
  constructor(props) {
    super(props);
    this.state = { expanded: props.expanded || false };
  }

  _stateMachine(events) {
    const states = events.map((e, i) => {
      return {
        className: addClass(
          getClassForUpdateAction(e.action),
          (i === events.length - 1) ? ' active' : ''),
        state: UPDATE_ACTION[e.action],
        timestamp: e.timestampMs
      };
    });

    return (<div className='update-instance-history'>
      <StateMachine
        className={getClassForUpdateAction(events[events.length - 1].action)}
        states={states} />
    </div>);
  }

  expand() {
    this.setState({ expanded: !this.state.expanded });
  }

  render() {
    const {events, instanceId, jobKey: {job: {role, environment, name}}} = this.props;
    const sorted = sort(events, (e) => e.timestampMs);
    const stateMachine = this.state.expanded ? this._stateMachine(sorted) : '';
    const icon = this.state.expanded ? <Icon name='chevron-down' /> : <Icon name='chevron-right' />;
    const latestEvent = sorted[sorted.length - 1];
    return (<div className='update-instance-event-container'>
      <div className='update-instance-event' onClick={(e) => this.expand()}>
        {icon}
        <span className='update-instance-event-id'>
          <Link to={`/scheduler/${role}/${environment}/${name}/${instanceId}`}>
            #{instanceId}
          </Link>
        </span>
        <span className='update-instance-event-status'>
          {UPDATE_ACTION[latestEvent.action]}
          <span className={getClassForUpdateAction(latestEvent.action)}>
            {instanceEventIcon(latestEvent)}
          </span>
        </span>
        <span className='update-instance-event-time'>
          {moment(latestEvent.timestampMs).utc().format('HH:mm:ss') + ' UTC'}
        </span>
      </div>
      {stateMachine}
    </div>);
  }
};

export default function UpdateInstanceEvents({ update }) {
  const sortedEvents = sort(update.instanceEvents, (e) => e.timestampMs, true);
  const instanceMap = {};
  const eventOrder = [];
  sortedEvents.forEach((e) => {
    const existing = instanceMap[e.instanceId];
    if (existing) {
      instanceMap[e.instanceId].push(e);
    } else {
      eventOrder.push(e.instanceId);
      instanceMap[e.instanceId] = [e];
    }
  });

  return (<div className='instance-events'>
    <Pagination
      data={eventOrder}
      hideIfSinglePage
      numberPerPage={10}
      renderer={(instanceId) => <InstanceEvent
        events={instanceMap[instanceId]}
        instanceId={instanceId}
        jobKey={update.update.summary.key} />} />
  </div>);
}
