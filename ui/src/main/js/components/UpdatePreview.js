import React from 'react';
import { Link } from 'react-router-dom';

import PanelGroup, { Container } from 'components/Layout';
import { RelativeTime } from 'components/Time';

import { getClassForUpdateStatus, updateStats } from 'utils/Update';

export default function UpdatePreview({ update }) {
  const stats = updateStats(update);
  const {job: {role, environment, name}, id} = update.update.summary.key;
  return (<Container>
    <PanelGroup noPadding title=''>
      <div
        className={`update-preview ${getClassForUpdateStatus(update.update.summary.state.status)}`}>
        <Link
          to={`/beta/scheduler/${role}/${environment}/${name}/update/${id}`}>
         Update In Progress
        </Link>
        <span className='update-preview-details'>
          started by <strong>{update.update.summary.user}</strong> <span>
            <RelativeTime ts={update.update.summary.state.createdTimestampMs} /></span>
        </span>
        <span className='update-preview-progress'>
          {stats.instancesUpdated} / {stats.totalInstancesToBeUpdated} ({stats.progress}%)
        </span>
      </div>
    </PanelGroup>
  </Container>);
}
