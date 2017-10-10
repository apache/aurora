import React from 'react';

import InstanceViz from 'components/InstanceViz';

import { instanceSummary, updateStats } from 'utils/Update';

function UpdateStats({ update }) {
  const stats = updateStats(update);
  return (<div className='update-summary-stats'>
    <h5>Instance Summary</h5>
    <span className='stats'>
      {stats.instancesUpdated} / {stats.totalInstancesToBeUpdated} ({stats.progress}%)
    </span>
  </div>);
};

export default function UpdateInstanceSummary({ update }) {
  return (<div>
    <UpdateStats update={update} />
    <InstanceViz instances={instanceSummary(update)} jobKey={update.update.summary.key} />
  </div>);
}
