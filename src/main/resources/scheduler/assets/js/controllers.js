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
  /* global ScheduleStatus:false, JobUpdateKey:false, JobUpdateQuery:false, JobKey:false */
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

  var REFRESH_RATES = {
    IN_PROGRESS_UPDATE_MS: 15000,
    COMPLETED_UPDATES_MS: 60000
  };

  auroraUIControllers.controller('RoleSummaryController',
    function ($scope, auroraClient) {
      $scope.title = 'Scheduled Jobs Summary';

      $scope.error = '';

      auroraClient.getRoleSummary().then(function (roleSummaries) {
        $scope.roleSummaries = parseResponse(roleSummaries);

        $scope.roleSummaryColumns = [
          {label: 'Role', map: 'role', cellTemplateUrl: '/assets/roleLink.html'},
          {label: 'Jobs', map: 'jobCount'},
          {label: 'Cron Jobs', map: 'cronJobCount'}
        ];

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
        $scope.roleSummaryTableConfig.columnSpan = $scope.roleSummaryColumns.length;
      });
    });

  auroraUIControllers.controller('JobSummaryController',
    function ($scope, $routeParams, auroraClient) {
      $scope.role = $routeParams.role;
      $scope.environment = $routeParams.environment;

      $scope.error = '';

      $scope.jobsTableColumns = [
        {label: 'Job Type', map: 'jobType'},
        {label: 'Environment', map: 'environment', cellTemplateUrl: '/assets/roleEnvLink.html'},
        {label: 'Job', map: 'jobName', cellTemplateUrl: '/assets/jobLink.html'},
        {label: 'production', map: 'isProduction'},
        {label: 'Pending Tasks', map: 'pendingTasks'},
        {label: 'Active Tasks', map: 'activeTasks'},
        {label: 'Finished Tasks', map: 'finishedTasks'},
        {label: 'Failed Tasks', map: 'failedTasks'}
      ];

      $scope.jobsTableConfig = infoTableConfig;
      $scope.jobsTableConfig.columnSpan = $scope.jobsTableColumns.length;

      function getJobType(job) {
        if (job.taskConfig.isService) {
          return 'service';
        }

        if (job.cronSchedule !== null) {
          return 'cron';
        }

        return 'adhoc';
      }

      function getJobs(summaries) {
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

        return _.chain(jobSummaries)
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

      auroraClient.getJobSummary($scope.role).then(function (summaries) {
        $scope.jobs = getJobs(summaries);
      });

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


      auroraClient.getQuota($scope.role).then(function (quotaResponse) {
        $scope.resources = getQuota(quotaResponse);
      });

      function getQuota(quotaResponse) {
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

  auroraUIControllers.controller('CronJobSummaryController',
    function ($scope, $filter, cronJobSummaryService) {
      $scope.cronJobSummaryTableConfig = summaryTableConfig;

      $scope.cronJobSummaryTableColumns = [
        {label: 'Number of tasks', map: 'tasks', isSortable: false},
        {label: 'Cron Schedule', map: 'schedule', isSortable: false},
        {label: 'Next Cron Run', map: 'nextCronRun', isSortable: false},
        {label: 'Collision Policy', map: 'collisionPolicy', isSortable: false},
        {label: 'Metadata', map: 'metadata', isSortable: false}
      ];

      $scope.error = '';
      $scope.cronJobSummary = [];

      cronJobSummaryService.getCronJobSummary($scope.role, $scope.environment,
       $scope.job).then(function (cronJobSummary) {
        if (cronJobSummary.error) {
          $scope.error = 'Error fetching cron job summary: ' + cronJobSummary.error;
          return [];
        }

        if (cronJobSummary.cronJobSummary) {
          var nextCronRunTs = cronJobSummary.cronJobSummary.nextCronRun;
          cronJobSummary.cronJobSummary.nextCronRun =
            $filter('toLocalTime')(nextCronRunTs) + ', ' + $filter('toUtcTime')(nextCronRunTs);

          $scope.cronJobSummary = [cronJobSummary.cronJobSummary];
        }
      });
    }
  );

  auroraUIControllers.controller('AllUpdatesController',
    function ($scope, $timeout, auroraClient, updateUtil) {

      function inProgressUpdates() {
        // fetch any in progress updates
        auroraClient.getJobUpdateSummaries(updateUtil.getInProgressQuery())
          .then(function (response) {
            $scope.inProgressUpdates = response.summaries;
            $timeout(function () { inProgressUpdates(); }, REFRESH_RATES.IN_PROGRESS_UPDATE_MS);
          });
      }

      function completedUpdates() {
        // fetch the latest 20 finished updates
        var finishedQuery = updateUtil.getTerminalQuery();
        finishedQuery.limit = 20;

        auroraClient.getJobUpdateSummaries(finishedQuery).then(function (response) {
          $scope.completedUpdates = response.summaries;
          $timeout(function () { completedUpdates(); }, REFRESH_RATES.COMPLETED_UPDATES_MS);
        });
      }

      inProgressUpdates();
      completedUpdates();
    }
  );

  auroraUIControllers.controller('UpdateController',
    function ($scope, $routeParams, $timeout, auroraClient, updateUtil) {
      var updateKey = new JobUpdateKey();
      updateKey.job = new JobKey();
      updateKey.job.role = $routeParams.role;
      updateKey.job.environment = $routeParams.environment;
      updateKey.job.name = $routeParams.job;
      updateKey.id = $routeParams.update;

      $scope.role = $routeParams.role;
      $scope.environment = $routeParams.environment;
      $scope.job = $routeParams.job;

      $scope.instanceSummary = [];

      var getUpdateProgress = function () {
        auroraClient.getJobUpdateDetails(updateKey).then(function (response) {
          $scope.update = response.details;
          $scope.inProgress = updateUtil.isInProgress($scope.update.update.summary.state.status);

          var duration = $scope.update.update.summary.state.lastModifiedTimestampMs -
            $scope.update.update.summary.state.createdTimestampMs;

          $scope.duration = moment.duration(duration).humanize();

          $scope.stats = updateUtil.getUpdateStats($scope.update);
          $scope.configJson = JSON
            .stringify($scope.update.update.instructions.desiredState.task, undefined, 2);

          // pagination for instance events
          var instanceEvents = $scope.instanceEvents = $scope.update.instanceEvents;
          $scope.eventsPerPage = 10;
          $scope.changeInstancePage = function () {
            var start = ($scope.currentPage - 1) * $scope.eventsPerPage;
            var end   = start + $scope.eventsPerPage;
            $scope.instanceEvents = instanceEvents.slice(start, end);
          };
          $scope.totalEvents = instanceEvents.length;
          $scope.currentPage = 1;
          $scope.changeInstancePage();

          // Instance summary display.
          $scope.instanceSummary = updateUtil.fillInstanceSummary($scope.update, $scope.stats);

          if ($scope.instanceSummary.length <= 20) {
            $scope.instanceGridSize = 'big';
          } else if ($scope.instanceSummary.length <= 1000) {
            $scope.instanceGridSize = 'medium';
          } else {
            $scope.instanceGridSize = 'small';
          }

          // Poll for updates while this update is in progress.
          if ($scope.inProgress) {
            $timeout(function () {
              getUpdateProgress();
            }, REFRESH_RATES.IN_PROGRESS_UPDATE_MS);
          }
        });
      };

      getUpdateProgress();
    }
  );

  auroraUIControllers.controller('JobController',
    function ($scope, $routeParams, $timeout, $q, auroraClient, taskUtil, updateUtil) {
      $scope.error = '';

      $scope.role = $routeParams.role;
      $scope.environment = $routeParams.environment;
      $scope.job = $routeParams.job;

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
        {label: 'Status', map: 'status', cellTemplateUrl: '/assets/taskStatus.html'},
        {label: 'Host', map: 'host', cellTemplateUrl: '/assets/taskSandbox.html'}
      ];

      var completedTaskColumns = addColumn(2,
        taskColumns,
        {label: 'Running duration',
          map: 'duration',
          formatFunction: function (duration) {
            return moment.duration(duration).humanize();
          }
        });

      var taskIdColumn = {
        label: 'Task ID',
        map: 'taskId',
        cellTemplateUrl: '/assets/taskLink.html'
      };

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

        $scope.activeTasksTableColumns = $scope.showTaskInfoLink ?
          addColumn(2, taskColumns, taskIdColumn) :
          taskColumns;

        $scope.completedTasksTableColumns = $scope.showTaskInfoLink ?
          addColumn(3, completedTaskColumns, taskIdColumn) :
          completedTaskColumns;
      };

      $scope.jobDashboardUrl = '';
      // These two task arrays need to be initialized due to a quirk in SmartTable's behavior.
      $scope.activeTasks = [];
      $scope.completedTasks = [];
      $scope.tasksReady = false;

      function buildGroupSummary(response) {

        if (response.error) {
          $scope.error = 'Error fetching configuration summary: ' + response.error;
          return [];
        }

        var colors = [
          'steelblue',
          'darkseagreen',
          'sandybrown',
          'plum',
          'khaki'
        ];

        var total = _.reduce(response.groups, function (m, n) {
          return m + n.instanceIds.length;
        }, 0);

        $scope.groupSummary = response.groups.map(function (group, i) {
          var count = group.instanceIds.length;
          var percentage = (count / total) * 100;

          var ranges = taskUtil.toRanges(group.instanceIds).map(function (r) {
            return (r.start === r.end) ? r.start : r.start + '-' + r.end;
          });

          return {
            label: ranges.join(', '),
            value: count,
            percentage: percentage,
            summary: { schedulingDetail: taskUtil.configToDetails(group.config)},
            color: colors[i % colors.length]
          };
        });
      }

      auroraClient.getTasksWithoutConfigs($scope.role, $scope.environment, $scope.job)
        .then(getTasksForJob);

      auroraClient.getConfigSummary($scope.role, $scope.environment, $scope.job)
        .then(buildGroupSummary);

      var query = new JobUpdateQuery();
      var jobKey = new JobKey();
      jobKey.role = $scope.role;
      jobKey.environment = $scope.environment;
      jobKey.name = $scope.job;
      query.jobKey = jobKey;

      auroraClient.getJobUpdateSummaries(query).then(getUpdatesForJob);


      function getUpdatesForJob(response) {
        $scope.updates = response.summaries;

        function getUpdateInProgress() {
          auroraClient.getJobUpdateDetails($scope.updates[0].key).then(function (response) {
            $scope.updateInProgress = response.details;

            $scope.updateStats = updateUtil.getUpdateStats($scope.updateInProgress);

            if (updateUtil.isInProgress($scope.updateInProgress.update.summary.state.status)) {
              // Poll for updates as long as this update is in progress.
              $timeout(function () {
                getUpdateInProgress();
              }, REFRESH_RATES.IN_PROGRESS_UPDATE_MS);
            }
          });
        }

        if ($scope.updates.length > 0 && updateUtil.isInProgress($scope.updates[0].state.status)) {
          getUpdateInProgress();
        }
      }

      function getTasksForJob(response) {
        if (response.error) {
          $scope.error = 'Error fetching tasks: ' + response.error;
          return [];
        }

        $scope.jobDashboardUrl = getJobDashboardUrl(response.statsUrlPrefix);

        var tasks = _.map(response.tasks, function (task) {
          return summarizeTask(task);
        });

        var activeTaskPredicate = function (task) {
          return task.isActive;
        };

        $scope.activeTasks = _.chain(tasks)
          .filter(activeTaskPredicate)
          .sortBy('instanceId')
          .value();

        $scope.completedTasks = _.chain(tasks)
          .reject(activeTaskPredicate)
          .sortBy(function (task) {
            return -task.latestActivity; //sort in descending order
          })
          .value();

        $scope.tasksReady = true;
      }

      function summarizeTask(task) {
        var isActive = taskUtil.isActiveTask(task);
        var sortedTaskEvents = _.sortBy(task.taskEvents, function (taskEvent) {
          return taskEvent.timestamp;
        });

        var latestTaskEvent = _.last(sortedTaskEvents);

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
          // TODO(maxim): Revisit this approach when the UI fix in AURORA-715 is finalized.
          sandboxExists: true
        };
      }

      function getDuration(sortedTaskEvents) {
        var runningTaskEvent = _.find(sortedTaskEvents, function (taskEvent) {
          return taskEvent.status === ScheduleStatus.RUNNING;
        });

        if (runningTaskEvent) {
          var nextEvent = sortedTaskEvents[_.indexOf(sortedTaskEvents, runningTaskEvent) + 1];

          return nextEvent ?
            nextEvent.timestamp - runningTaskEvent.timestamp :
            moment().valueOf() - runningTaskEvent.timestamp;
        }

        return 0;
      }

      function summarizeTaskEvents(taskEvents) {
        return _.map(taskEvents, function (taskEvent) {
          return {
            timestamp: taskEvent.timestamp,
            status: _.invert(ScheduleStatus)[taskEvent.status],
            message: taskEvent.message
          };
        });
      }

      function getJobDashboardUrl(statsUrlPrefix) {
        return _.isEmpty(statsUrlPrefix) ?
          '' :
          statsUrlPrefix + $scope.role + '.' + $scope.environment + '.' + $scope.job;
      }
    }
  );
})();
