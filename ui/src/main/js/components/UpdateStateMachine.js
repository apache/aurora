import React from 'react';

import StateMachine from 'components/StateMachine';

import { addClass } from 'utils/Common';
import { UPDATE_STATUS } from 'utils/Thrift';
import { getClassForUpdateStatus } from 'utils/Update';

export default function UpdateStateMachine({ update }) {
  const events = update.updateEvents;
  const states = events.map((e, i) => ({
    className: addClass(
      getClassForUpdateStatus(e.status),
      (i === events.length - 1) ? ' active' : ''),
    state: UPDATE_STATUS[e.status],
    message: e.message,
    timestamp: e.timestampMs
  }));
  const className = getClassForUpdateStatus(events[events.length - 1].status);
  return <StateMachine className={addClass('update-state-machine', className)} states={states} />;
}
