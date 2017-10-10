import moment from 'moment';
import React from 'react';

export default function UpdateSettings({ update }) {
  const settings = update.update.instructions.settings;
  return (<div>
    <table className='update-settings'>
      <tr>
        <td>Batch Size</td>
        <td>{settings.updateGroupSize}</td>
      </tr>
      <tr>
        <td>Max Failures Per Instance</td>
        <td>{settings.maxPerInstanceFailures}</td>
      </tr>
      <tr>
        <td>Max Failed Instances</td>
        <td>{settings.maxFailedInstances}</td>
      </tr>
      <tr>
        <td>Minimum Waiting Time in Running</td>
        <td>{moment.duration(settings.minWaitInInstanceRunningMs).humanize()}</td>
      </tr>
      <tr>
        <td>Rollback On Failure?</td>
        <td>{settings.rollbackOnFailure ? 'yes' : 'no'}</td>
      </tr>
    </table>
  </div>);
}
