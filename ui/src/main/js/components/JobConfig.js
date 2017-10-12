import React from 'react';

import ConfigDiff from 'components/ConfigDiff';
import Loading from 'components/Loading';
import TaskConfigSummary from 'components/TaskConfigSummary';

import { isNully, sort } from 'utils/Common';

export default function JobConfig({ groups }) {
  if (isNully(groups)) {
    return <Loading />;
  }

  const sorted = sort(groups, (g) => g.instances[0].first);
  return (<div className='job-configuration'>
    <div className='job-configuration-summaries'>
      {sorted.map((group, i) => <TaskConfigSummary key={i} {...group} />)}
    </div>
    <ConfigDiff groups={sorted} />
  </div>);
}
