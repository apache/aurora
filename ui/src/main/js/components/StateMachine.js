import React from 'react';
import moment from 'moment';

function StateItem({ className, state, message, timestamp }) {
  return (<li className={className}>
    <div className='state-machine-item'>
      <svg><circle className='state-machine-bullet' cx={6} cy={6} r={5} /></svg>
      <div className='state-machine-item-details'>
        <span className='state-machine-item-state'>{state}</span>
        <span className='state-machine-item-time'>
          {moment(timestamp).utc().format('MM/DD HH:mm:ss') + ' UTC'}<br />
          ({moment(timestamp).fromNow()})
        </span>
        <span className='state-machine-item-message'>{message}</span>
      </div>
    </div>
  </li>);
}

export class StateMachineToggle extends React.Component {
  constructor(props) {
    super(props);
    this.state = { expanded: this.props.expanded || false };
  }

  render() {
    const states = this.state.expanded ? this.props.states : [this.props.toggleState];
    return (<div onClick={(e) => this.setState({expanded: !this.state.expanded})}>
      <StateMachine className={this.props.className} states={states} />
    </div>);
  }
}

export default function StateMachine({ className, states }) {
  return (<div className='state-machine'>
    <ul className={className}>
      {states.map((s, i) => <StateItem key={i} {...s} />)}
    </ul>
  </div>);
}
