import React from 'react';
import { Link } from 'react-router-dom';

import Loading from 'components/Loading';
import Pagination from 'components/Pagination';
import { RelativeTime } from 'components/Time';

import { isNully } from 'utils/Common';
import { UPDATE_STATUS } from 'utils/Thrift';
import { getClassForUpdateStatus } from 'utils/Update';

function UpdateListItem({ summary }) {
  const {job: {role, environment, name}, id} = summary.key;
  return (<div className='update-list-item'>
    <span className={`img-circle ${getClassForUpdateStatus(summary.state.status)}`} />
    <div className='update-list-item-details'>
      <span className='update-list-item-status'>
        <Link
          className='update-list-job'
          to={`/beta/scheduler/${role}/${environment}/${name}/update/${id}`}>
          {role}/{environment}/{name}
        </Link> &bull; <span className='update-list-status'>
          {UPDATE_STATUS[summary.state.status]}
        </span>
      </span>
      started by <span className='update-list-user'>
        {summary.user} </span> <RelativeTime ts={summary.state.createdTimestampMs} />
    </div>
    <span className='update-list-last-updated'>
      updated <RelativeTime ts={summary.state.lastModifiedTimestampMs} />
    </span>
  </div>);
}

export default function UpdateList({ updates }) {
  if (isNully(updates)) {
    return <Loading />;
  }

  return (<div className='update-list'>
    <Pagination
      data={updates}
      hideIfSinglePage
      numberPerPage={25}
      renderer={(u) => <UpdateListItem key={u.key.id} summary={u} />} />
  </div>);
}
