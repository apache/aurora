import moment from 'moment';
import React from 'react';

export function RelativeTime({ ts }) {
  return <span>{moment(ts).fromNow()}</span>;
}
