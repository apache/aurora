import React from 'react';

import ConfigDiff from 'components/ConfigDiff';
import Loading from 'components/Loading';
import TaskConfigSummary, { CronConfigSummary } from 'components/TaskConfigSummary';

import { isNully, sort } from 'utils/Common';

export function CronJobConfig({ cronJob }) {
  return (<div className='job-configuration'>
    <div className='job-configuration-summaries'>
      <CronConfigSummary cronJob={cronJob} />
    </div>
  </div>);
}

export default function JobConfig({ cronJob, groups }) {
  if (isNully(groups) && isNully(cronJob)) {
    return <Loading />;
  }

  if (!isNully(cronJob)) {
    return <CronJobConfig cronJob={cronJob} />;
  }

  const sorted = sort(groups, (g) => g.instances[0].first);
  return (<div className='job-configuration'>
    <div className='job-configuration-summaries'>
      {sorted.map((group, i) => <TaskConfigSummary key={i} {...group} />)}
    </div>
    <ConfigDiff groups={sorted} />
  </div>);
}
