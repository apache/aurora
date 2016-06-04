/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function () {
  /* global auroraUI:false, JobUpdateStatus: false, JobUpdateAction: false */
  'use strict';

  auroraUI.filter('scheduleStatusTooltip', function () {
    var STATES = {
      PENDING: 'The scheduler is searching for a machine that satisfies the resources and ' +
        'constraints for this task.',

      THROTTLED: 'The task will be rescheduled, but is being throttled for restarting too ' +
        'frequently.',

      ASSIGNED: 'The scheduler has selected a machine to run the task and is instructing the ' +
        'agent to launch it.',

      STARTING: 'The executor is preparing to launch the task.',
      RUNNING: 'The user process(es) are running.',
      FAILED: 'The task ran, but did not exit indicating success.',
      FINISHED: 'The task ran and exited successfully.',
      KILLED: 'A user or cron invocation terminated the task.',
      PREEMPTING: 'This task is being killed to make resources available for a production task.',
      KILLING: 'A user request or cron invocation has requested the task be killed.',
      LOST: 'The task cannot be accounted for, usually a result of agent process or machine ' +
        'failure.',
      DRAINING: 'The task is being restarted since the host is undergoing scheduled maintenance.'
    };

    return function (value) {
      return STATES[value] ? STATES[value] : value;
    };
  });

  auroraUI.filter('toNiceStatus', function () {
    var updateStatusLookup = _.invert(JobUpdateStatus);

    var STATUS_MAP = {
      'ROLLED_FORWARD': 'SUCCESS',
      'ROLLING_FORWARD': 'IN PROGRESS',
      'ROLLING_BACK': 'ROLLING BACK',
      'ROLL_BACK_PAUSED': 'ROLL BACK PAUSED',
      'ROLL_FORWARD_PAUSED': 'PAUSED',
      'ROLLED_BACK': 'ROLLED BACK'
    };

    return function (status) {
      status = updateStatusLookup[status] || 'UNKNOWN';
      return STATUS_MAP.hasOwnProperty(status) ? STATUS_MAP[status] : status;
    };
  });

  auroraUI.filter('toNiceAction', function () {
    var instanceActionLookup = _.invert(JobUpdateAction);

    return function (action) {
      return (instanceActionLookup[action] || 'UNKNOWN')
        .replace(/INSTANCE_/, '')
        .replace(/_/g, ' ');
    };
  });

  auroraUI.filter('toNiceRanges', function () {
    return function (ranges) {
      return ranges.sort(function (a, b) { return a.first - b.first; }).map(function (range) {
        return range.first === range.last ? '' + range.first : range.first + '-' + range.last;
      }).join(', ');
    };
  });

  auroraUI.filter('toResourceValue', function () {
    var SCALE = ['MiB', 'GiB', 'TiB', 'PiB', 'EiB'];

    function formatMem(sizeInMb) {
      var size = sizeInMb;
      var unit = 0;
      while (size >= 1024 && unit < SCALE.length) {
        size = size / 1024;
        unit++;
      }
      return size.toFixed(2).toString() + ' ' + SCALE[unit];
    }

    return function (resources, type) {
      var RESOURCE_MAP = {
        'CPUS': {
          field: 'numCpus',
          format: function (v) { return _.first(v)[this.field] + ' core(s)'; }
        },
        'RAM_MB': {
          field: 'ramMb',
          format: function (v) { return formatMem(_.first(v)[this.field]); }
        },
        'DISK_MB': {
          field: 'diskMb',
          format: function (v) { return formatMem(_.first(v)[this.field]); }
        },
        'PORTS': {
          field: 'namedPort',
          format: function (v) {
            var field = this.field;
            return _.chain(v)
                .map(function (r) { return r[field]; })
                .sortBy()
                .value()
                .join(', ');
          }
        },
        'GPUS': {
          field: 'numGpus',
          format: function (v) { return _.first(v)[this.field] + ' core(s)'; }
        }
      };

      if (!type) {
        return _.chain(resources)
            .groupBy(function (r) {
              for (var key in RESOURCE_MAP) {
                var field = RESOURCE_MAP[key].field;
                if (r.hasOwnProperty(field) && r[field] !== null) {
                  return field;
                }
              }
              return null;
            })
            .size()
            .value();
      } else if (RESOURCE_MAP.hasOwnProperty(type)) {
        var field = RESOURCE_MAP[type].field;
        var match = _.filter(resources, function (r) { return r[field] !== null; });
        if (match && !_.isEmpty(match)) {
          return RESOURCE_MAP[type].format(match);
        }
      }

      return '';
    };
  });

  auroraUI.filter('toElapsedTime', function () {
    return function (timestamp) {
      return moment.duration(moment().valueOf() - timestamp).humanize();
    };
  });

  auroraUI.filter('toUtcTime', function () {
    return function (timestamp, timezone) {
      return moment(timestamp).utc().format('MM/DD HH:mm:ss') + ' UTC';
    };
  });

  auroraUI.filter('toLocalTime', function () {
    return function (timestamp, timezone) {
      return moment(timestamp).format('MM/DD HH:mm:ss') + ' LOCAL';
    };
  });

  auroraUI.filter('toLocalDay', function () {
    return function (timestamp) {
      return moment(timestamp).format('ddd, MMM Do');
    };
  });

  auroraUI.filter('toLocalTimeOnly', function () {
    return function (timestamp) {
      return moment(timestamp).format('HH:mm');
    };
  });

})();
