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
  /* global JobUpdateKey:false, JobUpdateQuery:false, JobKey:false */
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

      $scope.resourcesTableConfig = summaryTableConfig;


      auroraClient.getQuota($scope.role).then(function (quotaResponse) {
        $scope.resources = getQuota(quotaResponse);

        var consumption = quotaResponse.quota;
        var columns = [
          {label: 'Resource', map: 'resource'},
          {label: 'Quota', map: 'quota'},
          {label: 'Quota Consumption', map: 'prodSharedConsumption'},
          {label: 'Production Dedicated Consumption', map: 'prodDedicatedConsumption'},
          {label: 'Non-Production Consumption', map: 'nonProdSharedConsumption'},
          {label: 'Non-Production Dedicated Consumption', map: 'nonProdDedicatedConsumption'}
        ];

        columns = _.filter(columns, function (column) {
          var vector = consumption[column.map];
          return !vector || vector.numCpus > 0 || vector.ramMb > 0 || vector.diskMb > 0;
        });
        $scope.resourcesTableColumns = columns;

        // Assuming the max column count of 6. Revisit this approach if that's no longer the case.
        $scope.resourceClass = 'col-md-' + (columns.length * 2);

      });

      function getQuota(quotaResponse) {
        $scope.error = quotaResponse.error ? 'Error fetching quota: ' + quotaResponse.error : '';

        if ($scope.error) {
          return [];
        }

        function addResourceVector(name, filterSpec, vector) {
          var consumption = quotaResponse.quota;
          return {
            resource: name,
            quota: $filter(filterSpec)(consumption.quota[vector]),
            prodSharedConsumption: $filter(filterSpec)(consumption.prodSharedConsumption[vector]),
            prodDedicatedConsumption:
              $filter(filterSpec)(consumption.prodDedicatedConsumption[vector]),
            nonProdSharedConsumption:
              $filter(filterSpec)(consumption.nonProdSharedConsumption[vector]),
            nonProdDedicatedConsumption:
              $filter(filterSpec)(consumption.nonProdDedicatedConsumption[vector])
          };
        }

        return [
          addResourceVector('CPU', 'toCores', 'numCpus'),
          addResourceVector('RAM', 'scaleMb', 'ramMb'),
          addResourceVector('Disk', 'scaleMb', 'diskMb')
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

  function initializeJobController($scope, $routeParams) {
    $scope.error = '';

    $scope.role = $routeParams.role;
    $scope.environment = $routeParams.environment;
    $scope.job = $routeParams.job;
    $scope.instance = $routeParams.instance;

    // These two task arrays need to be initialized due to a quirk in SmartTable's behavior.
    $scope.activeTasks = [];
    $scope.completedTasks = [];
    $scope.tasksReady = false;
  }

  function JobController(
      $scope,
      $routeParams,
      $timeout,
      $q,
      $location,
      auroraClient,
      taskUtil,
      updateUtil,
      jobTasksService) {

    initializeJobController($scope, $routeParams);

    $scope.showTaskInfoLink = false;
    $scope.jobDashboardUrl = '';

    function setTabState(tab) {
      $scope.isActive = tab === 'active';
      $scope.isCompleted = tab === 'completed';
      $scope.isAll = tab === 'all';
    }

    setTabState($location.search().tab || 'active');

    $scope.toggleTaskInfoLinkVisibility = function () {
      $scope.showTaskInfoLink = !$scope.showTaskInfoLink;

      $scope.activeTasksTableColumns = $scope.showTaskInfoLink ?
        jobTasksService.addColumn(
          'Status',
          jobTasksService.taskColumns,
          jobTasksService.taskIdColumn) :
        jobTasksService.taskColumns;

      $scope.completedTasksTableColumns = $scope.showTaskInfoLink ?
        jobTasksService.addColumn(
          'Running duration',
          jobTasksService.completedTaskColumns,
          jobTasksService.taskIdColumn) :
        jobTasksService.completedTaskColumns;
    };

    $scope.switchTab = function (tab) {
      $location.search('tab', tab);
      setTabState(tab);
    };

    jobTasksService.getTasksForJob($scope);

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

    auroraClient.getConfigSummary($scope.role, $scope.environment, $scope.job)
      .then(buildGroupSummary);

    var query = new JobUpdateQuery();
    query.jobKey = new JobKey({
        role: $scope.role,
        environment: $scope.environment,
        name: $scope.job
      });

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

  }

  auroraUIControllers.controller('JobController', JobController);

  var guidPattern = new RegExp(
      /^[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}$/);

  function JobInstanceController($scope, $routeParams, $location, jobTasksService) {
    if (guidPattern.test($routeParams.instance)) {
      $location.path(
        [
          'scheduler',
          $routeParams.role,
          $routeParams.environment,
          $routeParams.job,
          'update',
          $routeParams.instance
        ].join('/'));
      return;
    }

    initializeJobController($scope, $routeParams);
    jobTasksService.getTasksForJob($scope);

    $scope.completedTasksTableColumns = $scope.completedTasksTableColumns.filter(function (column) {
      return column.label !== 'Instance';
    });

    $scope.completedTasksTableColumns = jobTasksService.addColumn(
        'Status',
        $scope.completedTasksTableColumns,
        jobTasksService.taskIdColumn);
  }

  auroraUIControllers.controller('JobInstanceController', JobInstanceController);
})();
