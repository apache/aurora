import React from 'react';

import JobHistory from 'components/JobHistory';
import JobStatus from 'components/JobStatus';
import PanelGroup from 'components/Layout';
import Loading from 'components/Loading';
import Tabs from 'components/Tabs';

import { isNully } from 'utils/Common';

export default function JobOverview(props) {
  if (isNully(props.tasks)) {
    return <PanelGroup><Loading /></PanelGroup>;
  }

  return (<Tabs
    activeTab={props.queryParams.jobView}
    className='job-overview'
    onChange={props.onViewChange}>
    {JobStatus(props)}
    {JobHistory(props)}
  </Tabs>);
}
