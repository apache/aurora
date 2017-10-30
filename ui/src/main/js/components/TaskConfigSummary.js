import moment from 'moment';
import React from 'react';

import { RelativeTime } from 'components/Time';

import { formatMb } from 'utils/Quota';
import { constraintToString, getResource, getResources, instanceRangeToString } from 'utils/Task';
import { COLLISION_POLICY } from 'utils/Thrift';

// ESLint doesn't like React's new adjacent elements, so we need to disable it here
/* eslint-disable */
function Resources({ config }) {
  return [<tr>
    <th rowSpan='4'>Resources</th>
    <td>cpus</td>
    <td>{getResource(config.resources, 'numCpus').numCpus}</td>
  </tr>,
  <tr>
    <td>ram</td>
    <td>{formatMb(getResource(config.resources, 'ramMb').ramMb)}</td>
  </tr>,
  <tr>
    <td>disk</td>
    <td>{formatMb(getResource(config.resources, 'diskMb').diskMb)}</td>
  </tr>,
  <tr>
    <td>ports</td>
    <td>{getResources(config.resources, 'namedPort').map((r) => r.namedPort).join(', ')}</td>
  </tr>];
}
/* eslint-enable */

function Constraints({ config }) {
  return (<tr>
    <th>Constraints</th>
    <td colSpan='2'>
      {config.constraints.map((t) => (<span className='task-constraint' key={t.name}>
        {t.name}: {constraintToString(t.constraint)}
      </span>))}
    </td>
  </tr>);
}

function Metadata({ config }) {
  return (<tr>
    <th>Metadata</th>
    <td colSpan='2'>
      {config.metadata.map((m, i) => (<span className='task-metadata' key={`${m.key}-${i}`}>
        {m.key}: {m.value}
      </span>))}
    </td>
  </tr>);
}

export function CronConfigSummary({ cronJob }) {
  const config = cronJob.job.taskConfig;
  return (<table className='table table-bordered task-config-summary cron-config-summary'>
    <tbody>
      <tr>
        <th colSpan='100%'>
          Cron Job Configuration
        </th>
      </tr>
      <tr>
        <th>Cron Schedule</th>
        <td colSpan='2'>{cronJob.job.cronSchedule}</td>
      </tr>
      <tr>
        <th>Collision Policy</th>
        <td colSpan='2'>{COLLISION_POLICY[cronJob.job.cronCollisionPolicy]}</td>
      </tr>
      <tr>
        <th>Next Cron Run</th>
        <td colSpan='2'>
          {moment(cronJob.nextCronRunMs).utc().format('MMMM Do YYYY, h:mm:ss a')} UTC (
          <RelativeTime ts={cronJob.nextCronRunMs} />)
        </td>
      </tr>
      <tr>
        <th># Instances</th>
        <td colSpan='2'>{cronJob.job.instanceCount}</td>
      </tr>
      <Resources config={config} />
      <Constraints config={config} />
      <tr>
        <th>Tier</th>
        <td colSpan='2'>{config.tier}</td>
      </tr>
      <Metadata config={config} />
      <tr>
        <th>Contact</th>
        <td colSpan='2'>{config.contactEmail}</td>
      </tr>
    </tbody>
  </table>);
}

export default function TaskConfigSummary({ config, instances }) {
  return (<table className='table table-bordered task-config-summary'>
    <tbody>
      <tr>
        <th colSpan='100%'>
          Configuration for instance {instanceRangeToString(instances)}
        </th>
      </tr>
      <Resources config={config} />
      <Constraints config={config} />
      <tr>
        <th>Tier</th>
        <td colSpan='2'>{config.tier}</td>
      </tr>
      <tr>
        <th>Service</th>
        <td colSpan='2'>{config.isService ? 'true' : 'false'}</td>
      </tr>
      <Metadata config={config} />
      <tr>
        <th>Contact</th>
        <td colSpan='2'>{config.contactEmail}</td>
      </tr>
    </tbody>
  </table>);
}
