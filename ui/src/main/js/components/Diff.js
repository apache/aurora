import React from 'react';
import { diffJson } from 'diff';

export default function Diff({ left, right }) {
  const result = diffJson(left, right);
  return (<div className='diff-view'>
    {result.map((r, i) => (
      <span className={r.added ? 'added' : r.removed ? 'removed' : 'same'} key={i}>
        {r.value}
      </span>))}
  </div>);
}
