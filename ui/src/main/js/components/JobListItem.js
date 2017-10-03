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

  const envLink = (props.env) ? '' : (<span className='job-env'>
    <Link to={`/beta/scheduler/${role}/${environment}`}>{environment}</Link>
  </span>);

  return (<tr key={`${environment}/${name}`}>
    <td className='job-list-type' column='type'>
      <span className='job-tier'>
        {taskConfig.isService ? 'service' : (cronSchedule) ? 'cron' : 'adhoc'}
      </span>
    </td>
    <td className='job-list-name' column='name' value={name}>
      <h4>
        {envLink}
        <Link to={`/beta/scheduler/${role}/${environment}/${name}`}>
          {name}
          {taskConfig.production ? <Icon name='star' /> : ''}
        </Link>
      </h4>
    </td>
    <td className='job-list-stats' column='stats'>
      <JobTaskStats stats={stats} />
    </td>
  </tr>);
}
