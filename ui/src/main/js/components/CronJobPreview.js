import React from 'react';
import moment from 'moment';

import { RelativeTime } from 'components/Time';

export default function CronJobPreview({ cronJob }) {
  return (<div className='cron-job-preview'>
    <strong>Cron job</strong>, next run will be at <span>
      {moment(cronJob.nextCronRunMs).utc().format('MMMM Do YYYY, h:mm:ss a')} UTC (
      <RelativeTime ts={cronJob.nextCronRunMs} />)
    </span>
  </div>);
}
