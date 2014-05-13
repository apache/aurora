/**
 * Copyright 2014 Apache Software Foundation
 *
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
'use strict';

/* Controllers */

var auroraUIControllers = angular.module('auroraUI.controllers', []);

var infoTableConfig = {
  isGlobalSearchActivated: true,
  isPaginationEnabled: true,
  itemsByPage: 25,
  maxSize: 8,
  selectionMode: 'single'
};

var summaryTableConfig = {
  isPaginationEnabled: false,
  isGlobalSearchActivated: false,
  selectionMode: 'none'
};

auroraUIControllers.controller('RoleSummaryController',
  function ($scope, auroraClient) {
    $scope.title = 'Scheduled Jobs Summary';

    $scope.error = '';

    $scope.roleSummaryColumns = [
      {label: 'Role', map: 'role', cellTemplateUrl: 'roleLink.html'},
      {label: 'Jobs', map: 'jobCount'},
      {label: 'Cron Jobs', map: 'cronJobCount'}
    ];

    $scope.roleSummaries = parseResponse(auroraClient.getRoleSummary());

    function parseResponse(response) {
      $scope.error = response.error ? 'Error requesting role summary: ' + response.error : '';

      if ($scope.error) {
        return [];
      }

      // TODO(Suman Karumuri): Replace sort with defaultSortColumn once it lands
      // https://github.com/lorenzofox3/Smart-Table/pull/61
      return response.summaries.sort(function (a, b) {
        if (a.role.toLowerCase() > b.role.toLowerCase()) {
          return 1;
        }
        if (a.role.toLowerCase() < b.role.toLowerCase()) {
          return -1;
        }
        return 0;
      });
    }

    $scope.roleSummaryTableConfig = infoTableConfig;
  });

auroraUIControllers.controller('JobSummaryController',
  function ($scope, $routeParams, auroraClient) {
    $scope.role = $routeParams.role;
    $scope.environment = $routeParams.environment;

    $scope.error = '';

    $scope.jobsTableColumns = [
      {label: 'Job Type', map: 'jobType'},
      {label: 'Environment', map: 'environment', cellTemplateUrl: '/roleEnvLink.html'},
      {label: 'Job', map: 'jobName', cellTemplateUrl: '/jobLink.html'},
      {label: 'production', map: 'isProduction'},
      {label: 'Pending Tasks', map: 'pendingTasks'},
      {label: 'Active Tasks', map: 'activeTasks'},
      {label: 'Finished Tasks', map: 'finishedTasks'},
      {label: 'Failed Tasks', map: 'failedTasks'}
    ];

    $scope.cronJobsTableColumns = [
      {label: 'Environment', map: 'environment', cellTemplateUrl: '/roleEnvLink.html'},
      {label: 'Job Name', map: 'jobName'},
      {label: 'Tasks', map: 'tasks'},
      {label: 'Schedule', map: 'schedule'},
      {label: 'Next Run',
        map: 'nextCronRun',
        formatFunction: function (value, format) {
          return printDate(value);
        }
      },
      {label: 'Collision Policy', map: 'collisionPolicy'},
      {label: 'Metadata', map: 'metadata'}
    ];

    $scope.jobsTableConfig = infoTableConfig;

    $scope.cronJobsTableConfig = infoTableConfig;

    $scope.cronJobs = [];

    $scope.jobs = getJobs();

    function getJobs() {
      var summaries = auroraClient.getJobSummary($scope.role);
      $scope.error = summaries.error ? 'Error fetching job summaries: ' + summaries.error : '';

      if ($scope.error) {
        return [];
      }

      var jobSummaries = summaries.jobs;

      if ($scope.environment) {
        jobSummaries = _.filter(jobSummaries, function (summary) {
          return summary.job.key.environment === $scope.environment;
        });
      }

      var byJobName = function (summary) {
        return summary.jobName;
      };

      $scope.cronJobs = _.chain(jobSummaries)
        .filter(function (summary) {
          return isCronJob(summary.job);
        })
        .map(function (summary) {
          return {
            role: $scope.role, // required for roleEnvLink directive
            environment: summary.job.key.environment,
            jobName: summary.job.taskConfig.jobName,
            tasks: summary.job.instanceCount,
            schedule: summary.job.cronSchedule,
            nextCronRun: summary.nextCronRunMs,
            collisionPolicy: getCronCollisionPolicy(summary.job.cronCollisionPolicy),
            metadata: getMetadata(summary.job.taskConfig.metadata)
          };
        })
        .sortBy(byJobName)
        .value();

      return _.chain(jobSummaries)
        .reject(isCronJobWithNoTasks)
        .map(function (summary) {
          return {
            role: $scope.role, // required for roleEnvLink directive
            environment: summary.job.key.environment,
            jobName: summary.job.taskConfig.jobName,
            jobType: getJobType(summary.job),
            isProduction: summary.job.taskConfig.production ? 'yes' : '',
            pendingTasks: summary.stats.pendingTaskCount,
            activeTasks: summary.stats.activeTaskCount,
            finishedTasks: summary.stats.finishedTaskCount,
            failedTasks: summary.stats.failedTaskCount
          };
        })
        .sortBy(byJobName)
        .value();
    }

    function getJobType(job) {
      if (job.taskConfig.isService) {
        return 'service';
      }

      if (isCronJob(job)) {
        return 'cron';
      }

      return 'adhoc';
    }

    function isCronJob(job) {
      return job.cronSchedule !== null;
    }

    function isCronJobWithNoTasks(summary) {
      var stats = summary.stats;
      var taskCount = stats.pendingTaskCount
        + stats.activeTaskCount
        + stats.finishedTaskCount
        + stats.failedTaskCount;
      return isCronJob(summary.job) && taskCount === 0;
    }

    function getMetadata(attributes) {
      return _.map(attributes,function (attribute) {
        return attribute.key + ': ' + attribute.value;
      }).join(', ');
    }

    function getCronCollisionPolicy(cronCollisionPolicy) {
      return _.keys(CronCollisionPolicy)[cronCollisionPolicy ? cronCollisionPolicy : 0];
    }

    // TODO(Suman Karumuri): Replace printDate with a more user friendly directive.
    function printDate(timestamp) {
      function pad(number) {
        number = '' + number;
        if (number.length < 2) {
          return '0' + number;
        }
        return number;
      }

      var d = new Date(timestamp);
      return pad(d.getUTCMonth() + 1) + '/'
        + pad(d.getUTCDate()) + ' '
        + pad(d.getUTCHours()) + ':'
        + pad(d.getUTCMinutes()) + ':'
        + pad(d.getUTCSeconds())
        + ' UTC ('
        + pad(d.getMonth() + 1) + '/'
        + pad(d.getDate()) + ' '
        + pad(d.getHours()) + ':'
        + pad(d.getMinutes()) + ':'
        + pad(d.getSeconds())
        + ' local)';
    }
  });

auroraUIControllers.controller('QuotaController',
  function ($scope, $filter, auroraClient) {
    $scope.error = '';

    $scope.resourcesTableColumns = [
      {label: 'Resource', map: 'resource'},
      {label: 'Production Consumption', map: 'prodConsumption'},
      {label: 'Quota', map: 'quota'},
      {label: 'Non-Production Consumption', map: 'nonProdConsumption'}
    ];

    $scope.resourcesTableConfig = summaryTableConfig;

    $scope.resources = getQuota();

    function getQuota() {
      var quotaResponse = auroraClient.getQuota($scope.role);
      $scope.error = quotaResponse.error ? 'Error fetching quota: ' + quotaResponse.error : '';

      if ($scope.error) {
        return [];
      }

      var consumption = quotaResponse.quota;
      return [
        {
          resource: 'CPU',
          prodConsumption: $filter('toCores')(consumption.prodConsumption.numCpus),
          quota: $filter('toCores')(consumption.quota.numCpus),
          nonProdConsumption: $filter('toCores')(consumption.nonProdConsumption.numCpus)
        },
        {
          resource: 'RAM',
          prodConsumption: $filter('scaleMb')(consumption.prodConsumption.ramMb),
          quota: $filter('scaleMb')(consumption.quota.ramMb),
          nonProdConsumption: $filter('scaleMb')(consumption.nonProdConsumption.ramMb)
        },
        {
          resource: 'Disk',
          prodConsumption: $filter('scaleMb')(consumption.prodConsumption.diskMb),
          quota: $filter('scaleMb')(consumption.quota.diskMb),
          nonProdConsumption: $filter('scaleMb')(consumption.nonProdConsumption.diskMb)
        }
      ];
    }
  }
);

auroraUIControllers.controller('JobController',
  function ($scope, $routeParams, auroraClient, taskUtil) {
    $scope.error = '';

    $scope.role = $routeParams.role;
    $scope.environment = $routeParams.environment;
    $scope.job = $routeParams.job;

    $scope.taskSummary = [];
    $scope.taskSummaryTableColumns = [
      {label: 'Instances',
        map: 'range',
        isSortable: false,
        formatFunction: function (range) {
          return range.start === range.end ? range.start : range.start + '-' + range.end;
        }
      },
      {label: 'Details',
        map: 'schedulingDetail',
        isSortable: false,
        cellTemplateUrl: '/schedulingDetail.html'
      }
    ];

    $scope.taskSummaryTableConfig = summaryTableConfig;

    var showSummary = 'Show Summary';
    var hideSummary = 'Hide Summary';
    $scope.summaryButtonText = showSummary;
    $scope.showSummary = false;

    $scope.toggleSummaryVisibility = function () {
      $scope.showSummary = !$scope.showSummary;
      $scope.summaryButtonText = $scope.showSummary ? hideSummary : showSummary;
    };

    var taskTableConfig = {
      isGlobalSearchActivated: false,
      isPaginationEnabled: true,
      itemsByPage: 50,
      maxSize: 8,
      selectionMode: 'single'
    };

    $scope.activeTasksTableConfig = taskTableConfig;
    $scope.completedTasksTableConfig = taskTableConfig;

    var taskColumns = [
      {label: 'Instance', map: 'instanceId'},
      {label: 'Status', map: 'status', cellTemplateUrl: '/taskStatus.html'},
      {label: 'Host', map: 'host', cellTemplateUrl: '/taskSandbox.html'}
    ];

    var completedTaskColumns = addColumn(2,
      taskColumns,
      {label: 'Running duration',
        map: 'duration',
        formatFunction: function (duration) {
          return moment.duration(duration).humanize();
        }
      });

    var taskIdColumn = {label: 'Task ID', map: 'taskId', cellTemplateUrl: '/taskLink.html'};

    $scope.activeTasksTableColumns = taskColumns;

    $scope.completedTasksTableColumns = completedTaskColumns;

    function addColumn(idxPosition, currentColumns, newColumn) {
      return _.union(
        _.first(currentColumns, idxPosition),
        [newColumn],
        _.last(currentColumns, currentColumns.length - idxPosition));
    }

    $scope.showTaskInfoLink = false;

    $scope.toggleTaskInfoLinkVisibility = function () {
      $scope.showTaskInfoLink = !$scope.showTaskInfoLink;

      $scope.activeTasksTableColumns = $scope.showTaskInfoLink
        ? addColumn(2, taskColumns, taskIdColumn)
        : taskColumns;

      $scope.completedTasksTableColumns = $scope.showTaskInfoLink
        ? addColumn(3, completedTaskColumns, taskIdColumn) :
        completedTaskColumns;
    };

    $scope.jobDashboardUrl = '';

    $scope.completedTasks = [];

    $scope.activeTasks = getTasksForJob($scope.role, $scope.environment, $scope.job);

    function getTasksForJob(role, environment, job) {
      var response = auroraClient.getTasks(role, environment, job);

      if (response.error) {
        $scope.error = 'Error fetching tasks: ' + response.error;
        return [];
      }

      $scope.jobDashboardUrl = getJobDashboardUrl(response.statsUrlPrefix);

      $scope.taskSummary = taskUtil.summarizeActiveTaskConfigs(response.tasks);

      var tasks = _.map(response.tasks, function (task) {
        return summarizeTask(task);
      });

      var activeTaskPredicate = function (task) {
        return task.isActive;
      };

      $scope.completedTasks = _.chain(tasks)
        .reject(activeTaskPredicate)
        .sortBy(function (task) {
          return -task.latestActivity; //sort in descending order
        })
        .value();

      return _.chain(tasks)
        .filter(activeTaskPredicate)
        .sortBy(function (task) {
          return task.instanceId;
        })
        .value();
    }

    function summarizeTask(task) {
      var isActive = taskUtil.isActiveTask(task);
      var sortedTaskEvents = _.sortBy(task.taskEvents, function (taskEvent) {
        return taskEvent.timestamp;
      });

      // Since all task sandboxes are eventually garbage collected SANDBOX_DELETED doesn't indicate
      // the state of the task, so use the previous task event to determine task status.
      var latestTaskEvent = task.status === ScheduleStatus.SANDBOX_DELETED
        ? _.chain(sortedTaskEvents).last(2).first().value()
        : _.last(sortedTaskEvents);

      return {
        instanceId: task.assignedTask.instanceId,
        status: _.invert(ScheduleStatus)[latestTaskEvent.status],
        statusMessage: latestTaskEvent.message,
        host: task.assignedTask.slaveHost || '',
        latestActivity: _.isEmpty(sortedTaskEvents) ? 0 : latestTaskEvent.timestamp,
        duration: getDuration(sortedTaskEvents),
        isActive: isActive,
        taskId: task.assignedTask.taskId,
        taskEvents: summarizeTaskEvents(sortedTaskEvents),
        showDetails: false,
        sandboxExists: task.status !== ScheduleStatus.SANDBOX_DELETED
      };
    }

    function getDuration(sortedTaskEvents) {
      var runningTaskEvent = _.find(sortedTaskEvents, function (taskEvent) {
        return taskEvent.status === ScheduleStatus.RUNNING;
      });

      if (runningTaskEvent) {
        var nextEvent = sortedTaskEvents[_.indexOf(sortedTaskEvents, runningTaskEvent) + 1];

        return nextEvent
          ? nextEvent.timestamp - runningTaskEvent.timestamp
          : moment().valueOf() - runningTaskEvent.timestamp;
      }

      return 0;
    }

    function summarizeTaskEvents(taskEvents) {
      return _.map(taskEvents, function (taskEvent) {
        return {
          date: moment(taskEvent.timestamp).format('MM/DD h:mm:ss'),
          status: _.invert(ScheduleStatus)[taskEvent.status],
          message: taskEvent.message
        };
      });
    }

    function getJobDashboardUrl(statsUrlPrefix) {
      return _.isEmpty(statsUrlPrefix)
        ? ''
        : statsUrlPrefix + $scope.role + '.' + $scope.environment + '.' + $scope.job;
    }
  }
);
