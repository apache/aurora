import React from 'react';
import { diffJson } from 'diff';

import { clone } from 'utils/Common';
import { isThermos } from 'utils/Task';

function maybeParseThermos(task) {
  if (isThermos(task)) {
    const modifiedTask = clone(task);
    modifiedTask.executorConfig.data = JSON.parse(task.executorConfig.data);
    return modifiedTask;
  }
  return task;
}

export default function Diff({ left, right }) {
  const result = diffJson(maybeParseThermos(left), maybeParseThermos(right));
  return (<div className='diff-view'>
    {result.map((r, i) => (
      <span className={r.added ? 'added' : r.removed ? 'removed' : 'same'} key={i}>
        {r.value}
      </span>))}
  </div>);
}
