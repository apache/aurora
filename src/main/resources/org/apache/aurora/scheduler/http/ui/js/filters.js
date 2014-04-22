'use strict';

auroraUI.filter('scheduleStatusTooltip', function () {
  var STATES = {
    PENDING: 'The scheduler is searching for a machine that satisfies the resources and '
      + 'constraints for this task.',

    THROTTLED: 'The task will be rescheduled, but is being throttled for restarting too '
      + 'frequently.',

    ASSIGNED: 'The scheduler has selected a machine to run the task and is instructing the '
      + 'slave to launch it.',

    STARTING: 'The executor is preparing to launch the task.',
    RUNNING: 'The user process(es) are running.',
    FAILED: 'The task ran, but did not exit indicating success.',
    FINISHED: 'The task ran and exited successfully.',
    KILLED: 'A user or cron invocation terminated the task.',
    PREEMPTING: 'This task is being killed to make resources available for a production task.',
    KILLING: 'A user request or cron invocation has requested the task be killed.',
    LOST: 'The task cannot be accounted for, usually a result of slave process or machine '
      + 'failure.'
  };

  return function (value) {
    return STATES[value] ? STATES[value] : value;
  };
});

auroraUI.filter('scaleMb', function () {
  var SCALE = ['MiB', 'GiB', 'TiB', 'PiB', 'EiB'];

  return function (sizeInMb) {
    var size = sizeInMb;
    var unit = 0;
    while (size >= 1024 && unit < SCALE.length) {
      size = size / 1024;
      unit++;
    }
    return size.toFixed(2).toString() + ' ' + SCALE[unit];
  };
});

auroraUI.filter('toCores', function () {
  return  function (count) {
    return count + ' cores';
  }
});

auroraUI.filter('toElapsedTime', function () {
  return function (timestamp) {
    return moment.duration(moment().valueOf() - timestamp).humanize();
  }
});
