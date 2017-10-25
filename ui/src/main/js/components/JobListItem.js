import React from 'react';
import { Link } from 'react-router-dom';

import Icon from 'components/Icon';

import { TASK_COUNTS } from 'utils/Job';

export function JobTaskStats({ stats }) {
  const taskStats = [];
  TASK_COUNTS.forEach((k) => {
    if (stats[k] > 0) {
      const label = k.replace('TaskCount', '');
      taskStats.push(<li key={k}><span className={`img-circle ${label}-task`} /> {stats[k]} </li>);
    }
  });
  return <ul className='job-task-stats'>{taskStats}</ul>;
}

export default function JobListItem(props) {
  const {job: {job: { cronSchedule, key: {role, name, environment}, taskConfig }, stats}} = props;

  const envLink = (props.env) ? null : (<td className='job-list-env'>
    <Link to={`/scheduler/${role}/${environment}`}>{environment}</Link>
  </td>);

  return (<tr key={`${environment}/${name}`}>
    <td className='job-list-type'>
      {taskConfig.isService ? 'service' : (cronSchedule) ? 'cron' : 'adhoc'}
    </td>
    {envLink}
    <td className='job-list-name' value={name}>
      <h4>
        <Link to={`/scheduler/${role}/${environment}/${name}`}>
          {name}
          {taskConfig.production ? <Icon name='star' /> : ''}
        </Link>
      </h4>
    </td>
    <td className='job-list-stats'>
      <JobTaskStats stats={stats} />
    </td>
  </tr>);
}
