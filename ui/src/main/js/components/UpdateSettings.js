import moment from 'moment';
import React from 'react';

import { isNully } from '../utils/Common';

export default function UpdateSettings({ update }) {
  const settings = update.update.instructions.settings;

  return (<div>
    <table className='update-settings'>
      <tbody>
        <UpdateStrategy strategy={settings.updateStrategy} />
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
        <tr>
          <td>SLA-Aware?</td>
          <td>{settings.slaAware ? 'yes' : 'no'}</td>
        </tr>
      </tbody>
    </table>
  </div>);
}

// ESLint doesn't like React's new adjacent elements, so we need to disable it here
/* eslint-disable */
function UpdateStrategy({ strategy }) {
  if (isNully(strategy)) {
    return null;
  }

  if (!isNully(strategy.queueStrategy)) {
    return [<tr>
      <td>Strategy</td>
      <td>Queue</td>
    </tr>,
    <tr>
      <td>Max Parallel Updates</td>
      <td>{ strategy.queueStrategy.groupSize }</td>
    </tr>];
  } else if (!isNully(strategy.batchStrategy)) {
    return [<tr>
      <td>Strategy</td>
      <td>Batch</td>
    </tr>,
    <tr>
      <td>Batch Size</td>
      <td>{ strategy.batchStrategy.groupSize }</td>
    </tr>,
    <tr>
      <td>Auto Pause after Batch</td>
      <td>{ strategy.batchStrategy.autopauseAfterBatch ? 'yes' : 'no' }</td>
    </tr>];
  } else if (!isNully(strategy.varBatchStrategy)) {
    return [<tr>
      <td>Strategy</td>
      <td>Variable Batch</td>
    </tr>,
    <tr>
      <td>Batch Sizes</td>
      <td>{ strategy.varBatchStrategy.groupSizes.toString() }</td>
    </tr>,
    <tr>
      <td>Auto Pause after Batch</td>
      <td>{ strategy.varBatchStrategy.autopauseAfterBatch ? 'yes' : 'no' }</td>
    </tr>];
  }

  return null;
}

/* eslint-enable */
