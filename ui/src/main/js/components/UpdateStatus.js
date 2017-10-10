import React from 'react';

import UpdateTime from 'components/UpdateTime';

import { UPDATE_STATUS } from 'utils/Thrift';
import { isInProgressUpdate } from 'utils/Update';

export default function UpdateStatus({ update }) {
  const time = isInProgressUpdate(update) ? '' : <UpdateTime update={update} />;
  return (<div>
    <div className='update-byline'>
      <span>
        Update started by <strong>{update.update.summary.user}</strong>
      </span>
      <span>&bull;</span>
      <span>
        Status: <strong>{UPDATE_STATUS[update.update.summary.state.status]}</strong>
      </span>
    </div>
    {time}
  </div>);
}
