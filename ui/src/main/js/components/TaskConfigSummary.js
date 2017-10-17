import React from 'react';

import { constraintToString, getResource, getResources, instanceRangeToString } from 'utils/Task';

export default function TaskConfigSummary({ config, instances }) {
  return (<table className='table table-bordered task-config-summary'>
    <tbody>
      <tr>
        <th colSpan='100%'>
          Configuration for instance {instanceRangeToString(instances)}
        </th>
      </tr>
      <tr>
        <th rowSpan='4'>Resources</th>
        <td>cpus</td>
        <td>{getResource(config.resources, 'numCpus').numCpus}</td>
      </tr>
      <tr>
        <td>ram</td>
        <td>{getResource(config.resources, 'ramMb').ramMb}</td>
      </tr>
      <tr>
        <td>disk</td>
        <td>{getResource(config.resources, 'diskMb').diskMb}</td>
      </tr>
      <tr>
        <td>ports</td>
        <td>{getResources(config.resources, 'namedPort').map((r) => r.namedPort).join(', ')}</td>
      </tr>
      <tr>
        <th>Constraints</th>
        <td colSpan='2'>
          {config.constraints.map((t) => (<span className='task-constraint' key={t.name}>
            {t.name}: {constraintToString(t.constraint)}
          </span>))}
        </td>
      </tr>
      <tr>
        <th>Tier</th>
        <td colSpan='2'>{config.tier}</td>
      </tr>
      <tr>
        <th>Service</th>
        <td colSpan='2'>{config.isService ? 'true' : 'false'}</td>
      </tr>
      <tr>
        <th>Metadata</th>
        <td colSpan='2'>
          {config.metadata.map((m) => <span key={m.key}>{m.key}: {m.value}</span>)}
        </td>
      </tr>
      <tr>
        <th>Contact</th>
        <td colSpan='2'>{config.contactEmail}</td>
      </tr>
    </tbody>
  </table>);
}
