import moment from 'moment';
import React from 'react';

import { RelativeTime } from 'components/Time';

function UpdateTimeDisplay({ timestamp }) {
  return (<div className='update-time'>
    <span>{moment(timestamp).utc().format('ddd, MMM Do')}</span>
    <h4>{moment(timestamp).utc().format('HH:mm')}</h4>
    <span className='time-ago'><RelativeTime ts={timestamp} /></span>
  </div>);
};

function UpdateDuration({ update }) {
  const duration = (update.update.summary.state.lastModifiedTimestampMs -
    update.update.summary.state.createdTimestampMs);
  return <div className='update-duration'>Duration: {moment.duration(duration).humanize()}</div>;
};

function UpdateTimeRange({ update }) {
  return (<div className='update-time-range'>
    <UpdateTimeDisplay timestamp={update.update.summary.state.createdTimestampMs} />
    <h5>~</h5>
    <UpdateTimeDisplay timestamp={update.update.summary.state.lastModifiedTimestampMs} />
  </div>);
};

export default function UpdateTime({ update }) {
  return (<div>
    <UpdateTimeRange update={update} />
    <UpdateDuration update={update} />
  </div>);
}
