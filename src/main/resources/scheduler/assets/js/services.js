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
  /* global
    auroraUI:false,
    ACTIVE_STATES:false,
    ACTIVE_JOB_UPDATE_STATES: false,
    CronCollisionPolicy: false,
    Identity:false,
    JobKey: false,
    JobUpdateQuery:false,
    JobUpdateAction:false,
    JobUpdateStatus: false,
    ReadOnlySchedulerClient:false,
    ScheduleStatus: false,
    TaskQuery:false
  */
  'use strict';

  function makeJobTaskQuery(role, environment, jobName, instance) {
    var id = new Identity();
    id.role = role;
    var taskQuery = new TaskQuery();
    taskQuery.owner = id;
    taskQuery.environment = environment;
    taskQuery.jobName = jobName;

    if (instance) {
      taskQuery.instanceIds = [ instance ];
    }

    return taskQuery;
  }

  auroraUI.factory(
    'auroraClient',
    ['$window', '$q',
      function ($window, $q) {
        function async(fn) {
          var deferred = $q.defer();
          fn(deferred);
          return deferred.promise;
        }

        var auroraClient = {
          // Each of the functions below wrap an API call on the scheduler.
          getRoleSummary: function () {
            return async(function (deferred) {
              auroraClient.getSchedulerClient().getRoleSummary(function (response) {
                var result = auroraClient.processResponse(response);
                result.summaries = response.result !== null ?
                  response.result.roleSummaryResult.summaries : [];
                deferred.resolve(result);
              });
            });
          },

          getJobSummary: function (role) {
            return async(function (deferred) {
              auroraClient.getSchedulerClient().getJobSummary(role, function (response) {
                var result = auroraClient.processResponse(response);
                result.jobs = response.result !== null ?
                  response.result.jobSummaryResult.summaries : [];
                deferred.resolve(result);
              });
            });
          },

          getQuota: function (role) {
            return async(function (deferred) {
              auroraClient.getSchedulerClient().getQuota(role, function (response) {
                var result = auroraClient.processResponse(response);
                result.quota = response.result !== null ? response.result.getQuotaResult : [];
                deferred.resolve(result);
              });
            });
          },

          getTasks: function (role, environment, jobName) {
            var id = new Identity();
            id.role = role;
            var taskQuery = new TaskQuery();
            taskQuery.owner = id;
            taskQuery.environment = environment;
            taskQuery.jobName = jobName;
            return async(function (deferred) {
              auroraClient.getSchedulerClient().getTasksStatus(taskQuery, function (response) {
                var result = auroraClient.processResponse(response);
                result.tasks = response.result !== null ?
                  response.result.scheduleStatusResult.tasks : [];
                deferred.resolve(result);
              });
            });
          },

          getTasksWithoutConfigs: function (role, environment, jobName, instance) {
            var query = makeJobTaskQuery(role, environment, jobName, instance);

            return async(function (deferred) {
              var tasksPromise = async(function (d1) {
                auroraClient.getSchedulerClient().getTasksWithoutConfigs(query, function (rsp) {
                  var result = auroraClient.processResponse(rsp);
                  result.tasks = rsp.result !== null ?
                    rsp.result.scheduleStatusResult.tasks : [];
                  d1.resolve(result);
                });
              });

              var pendingPromise = async(function (d2) {
                auroraClient.getSchedulerClient().getPendingReason(query, function (response) {
                  var reasons = response.result !== null ?
                    response.result.getPendingReasonResult.reasons : [];
                  d2.resolve(reasons);
                });
              });

              $q.all([tasksPromise, pendingPromise]).then(function (responses) {
                var result = responses[0], reasons = responses[1];
                // Attach current pending reasons to any pending tasks we might have
                var pendingTasks = _.filter(result.tasks, function (t) {
                  return t.status === ScheduleStatus.PENDING;
                });

                if (pendingTasks.length > 0) {
                  reasons = _.indexBy(reasons, 'taskId');
                  pendingTasks.forEach(function (t) {
                    if (reasons.hasOwnProperty(t.assignedTask.taskId)) {
                      // find the latest task event (that is pending)
                      // and set the message to be this reason
                      var latestPending = _.chain(t.taskEvents)
                        .filter(function (e) {
                          return e.status === ScheduleStatus.PENDING;
                        })
                        .sortBy(function (e) {
                          return e.timestamp;
                        })
                        .last().value();

                      latestPending.message = reasons[t.assignedTask.taskId].reason;
                    }
                  });
                }

                deferred.resolve(result);
              });
            });
          },

          getConfigSummary: function (role, environment, jobName) {
            return async(function (deferred) {
              var key = new JobKey();
              key.role = role;
              key.environment = environment;
              key.name = jobName;
              auroraClient.getSchedulerClient().getConfigSummary(key, function (response) {
                var result = auroraClient.processResponse(response);
                result.groups = response.result !== null ?
                  response.result.configSummaryResult.summary.groups : [];
                deferred.resolve(result);
              });
            });
          },

          getJobUpdateSummaries: function (query) {
            return async(function (deferred) {
              query = query || new JobUpdateQuery();
              auroraClient.getSchedulerClient().getJobUpdateSummaries(query, function (response) {
                var result = auroraClient.processResponse(response);
                result.summaries = response.result !== null ?
                  response.result.getJobUpdateSummariesResult.updateSummaries : [];
                deferred.resolve(result);
              });
            });
          },

          getJobUpdateDetails: function (updateKey) {
            return async(function (deferred) {
              auroraClient.getSchedulerClient().getJobUpdateDetails(updateKey, function (response) {
                var result = auroraClient.processResponse(response);
                result.details = response.result !== null ?
                  response.result.getJobUpdateDetailsResult.details : {};
                deferred.resolve(result);
              });
            });
          },

          // Utility functions
          // TODO(Suman Karumuri): Make schedulerClient a service
          schedulerClient: null,

          getSchedulerClient: function () {
            if (!auroraClient.schedulerClient) {
              var transport = new Thrift.Transport('/api');
              var protocol = new Thrift.Protocol(transport);
              auroraClient.schedulerClient = new ReadOnlySchedulerClient(protocol);
              return auroraClient.schedulerClient;
            } else {
              return auroraClient.schedulerClient;
            }
          },

          processResponse: function (response) {
            auroraClient.setPageTitle(response.serverInfo);
            var error = response.responseCode !== 1 ?
                (response.message || 'No error message returned by the scheduler') : '',
              statsUrlPrefix = response.serverInfo && response.serverInfo.statsUrlPrefix ?
                response.serverInfo.statsUrlPrefix : '';

            return {
              error: error,
              statsUrlPrefix: statsUrlPrefix
            };
          },

          getPageTitle: function (info) {
            var title = 'Aurora';
            if (_.isNull(info) || info.error || typeof info.clusterName === 'undefined') {
              return title;
            } else {
              return '[' + info.clusterName + '] ' + title;
            }
          },

          setPageTitle: function (serverInfo) {
            $window.document.title = auroraClient.getPageTitle(serverInfo);
          }
        };

        return auroraClient;
      }
    ]);

  auroraUI.factory(
    'updateUtil',
    function () {
      function toSet(values) {
        var tmp = {};
        values.forEach(function (key) {
          tmp[key] = true;
        });
        return tmp;
      }

      // TODO(dmclaughlin): Make these constants in api.thrift.
      var UPDATE_TERMINAL_STATUSES = [
        JobUpdateStatus.ROLLED_FORWARD,
        JobUpdateStatus.ROLLED_BACK,
        JobUpdateStatus.ABORTED,
        JobUpdateStatus.ERROR,
        JobUpdateStatus.FAILED
      ];

      var UPDATE_TERMINAL = toSet(UPDATE_TERMINAL_STATUSES);

      var INSTANCE_SUCCESSFUL = toSet([
        JobUpdateAction.INSTANCE_UPDATED
      ]);

      var INSTANCE_TERMINAL = toSet([
        JobUpdateAction.INSTANCE_UPDATED,
        JobUpdateAction.INSTANCE_ROLLED_BACK,
        JobUpdateAction.INSTANCE_UPDATE_FAILED,
        JobUpdateAction.INSTANCE_ROLLBACK_FAILED
      ]);

      var instanceActionLookup = _.invert(JobUpdateAction);

      var updateUtil = {
        isTerminal: function (status) {
          return UPDATE_TERMINAL.hasOwnProperty(status);
        },
        isInProgress: function (status) {
          return ! updateUtil.isTerminal(status);
        },
        isInstanceSuccessful: function (action) {
          return INSTANCE_SUCCESSFUL.hasOwnProperty(action);
        },
        isInstanceTerminal: function (action) {
          return INSTANCE_TERMINAL.hasOwnProperty(action);
        },
        instanceCountFromRanges: function (ranges) {
          // add the deltas of remaining ranges
          // note - we don't check for overlapping ranges here
          // because that would be a bug in the scheduler
          var instanceCount = 0;

          ranges.forEach(function (r) {
            instanceCount += (r.last - r.first + 1);
          });

          return instanceCount;
        },
        getStatusQuery: function (statuses) {
          var query = new JobUpdateQuery();
          query.updateStatuses = statuses;
          return query;
        },
        getTerminalQuery: function () {
          return updateUtil.getStatusQuery(UPDATE_TERMINAL_STATUSES);
        },
        getInProgressQuery: function () {
          return updateUtil.getStatusQuery(ACTIVE_JOB_UPDATE_STATES);
        },
        instanceCountFromConfigs: function (instanceTaskConfigs) {
          var flattenedRanges = [];

          // get all ranges
          instanceTaskConfigs.forEach(function (iTaskConfig) {
            iTaskConfig.instances.forEach(function (range) {
              flattenedRanges.push(range);
            });
          });

          return updateUtil.instanceCountFromRanges(flattenedRanges);
        },
        progressFromEvents: function (instanceEvents) {
          var successful = updateUtil.getLatestInstanceEvents(instanceEvents, function (e) {
            return updateUtil.isInstanceSuccessful(e.action);
          });
          return Object.keys(successful).length;
        },
        displayClassForInstanceStatus: function (action) {
          return instanceActionLookup[action].toLowerCase().replace(/_/g, '-');
        },
        processInstanceIdsFromRanges: function (ranges, cb) {
          ranges.forEach(function (r) {
            for (var i = r.first; i <= r.last; i++) {
              cb(i);
            }
          });
        },
        getAllInstanceIds: function (update) {
          var allIds = {}, newIds = {}, oldIds = {};

          updateUtil.processInstanceIdsFromRanges(update.instructions.desiredState.instances,
            function (instanceId) {
              newIds[instanceId] = true;
              allIds[instanceId] = true;
            });

          update.instructions.initialState.forEach(function (iTaskConfig) {
            updateUtil.processInstanceIdsFromRanges(iTaskConfig.instances, function (instanceId) {
              oldIds[instanceId] = true;
              allIds[instanceId] = true;
            });
          });

          return {
            allIds: allIds,
            newIds: newIds,
            oldIds: oldIds
          };
        },
        getLatestInstanceEvents: function (instanceEvents, condition) {
          var events = _.sortBy(instanceEvents, 'timestampMs');
          var instanceMap = {};
          condition = condition || function () { return true; };

          for (var i = events.length - 1; i >= 0; i--) {
            if (!instanceMap.hasOwnProperty(events[i].instanceId) && condition(events[i])) {
              instanceMap[events[i].instanceId] = events[i];
            }
          }

          return instanceMap;
        },
        fillInstanceSummary: function (details, stats) {
          // get latest event for each instance
          var instanceMap = updateUtil.getLatestInstanceEvents(details.instanceEvents);

          // instances to show is the union of old instances and new instances.
          var allInstances = updateUtil.getAllInstanceIds(details.update);

          var allIds = Object.keys(allInstances.allIds);

          return allIds.map(function (i) {
            if (instanceMap.hasOwnProperty(i)) {
              var event = instanceMap[i];

              // If instance id is in initialState but not desiredState, and last
              // action is a successful update - it means that this instance was removed.
              if (updateUtil.isInstanceSuccessful(event.action) &&
                  allInstances.oldIds.hasOwnProperty(i) &&
                  !allInstances.newIds.hasOwnProperty(i)) {
                return {
                  instanceId: i,
                  className: 'instance-removed'
                };
              }

              // Normal case - just use the latest action from the instance events.
              var className = updateUtil.displayClassForInstanceStatus(event.action);
              return {
                instanceId: i,
                className: className,
                event: event
              };
            } else {
              // Otherwise it is pending an update.
              return {
                instanceId: i,
                className: 'pending'
              };
            }
          });
        },
        getUpdateStats: function (details) {
          if (!details || !details.update) {
            return {};
          }

          var allInstances = Object.keys(updateUtil.getAllInstanceIds(details.update).allIds);
          var totalInstancesToBeUpdated = allInstances.length;

          var instancesUpdated = updateUtil.progressFromEvents(details.instanceEvents);

          // calculate the percentage of work done so far
          var progress = Math.round((instancesUpdated / totalInstancesToBeUpdated) * 100);

          return {
            totalInstancesToBeUpdated: totalInstancesToBeUpdated,
            instancesUpdatedSoFar: instancesUpdated,
            progress: progress
          };
        }
      };

      return updateUtil;
    });

  auroraUI.factory(
    'taskUtil',
    function () {
      var taskUtil = {
        // Given a list of tasks, group tasks with identical task configs and belonging to
        // contiguous instance ids together.
        summarizeActiveTaskConfigs: function (tasks) {
          return _.chain(tasks)
            .filter(taskUtil.isActiveTask)
            .map(function (task) {
              return {
                instanceId: task.assignedTask.instanceId,
                schedulingDetail: taskUtil.configToDetails(task.assignedTask.task)
              };
            })
            .groupBy(function (task) {
              return JSON.stringify(task.schedulingDetail);
            })
            .map(function (tasks) {
              // Given a list of tasks with the same task config, group the tasks into ranges where
              // each range consists of consecutive task ids along with their task config.
              var schedulingDetail = _.first(tasks).schedulingDetail;
              var ranges = taskUtil.toRanges(_.pluck(tasks, 'instanceId'));
              return _.map(ranges, function (range) {
                return {
                  range: range,
                  schedulingDetail: schedulingDetail
                };
              });
            })
            .flatten(true)
            .sortBy(function (scheduleDetail) {
              return scheduleDetail.range.start;
            })
            .value();
        },

        configToDetails: function (task) {
          var constraints = _.chain(task.constraints)
            .sortBy(function (constraint) {
              return constraint.name;
            })
            .map(taskUtil.formatConstraint)
            .value()
            .join(', ');

          var metadata = _.chain(task.metadata)
            .sortBy(function (metadata) {
              return metadata.key;
            })
            .map(function (metadata) {
              return metadata.key + ':' + metadata.value;
            })
            .value()
            .join(', ');

          var container;
          if (task.container && task.container.docker) {
            container = {};
            container.image = task.container.docker.image;
          }

          return {
            numCpus: task.numCpus,
            ramMb: task.ramMb,
            diskMb: task.diskMb,
            isService: task.isService,
            production: task.production,
            contact: task.contactEmail || '',
            ports: _.sortBy(task.requestedPorts).join(', '),
            constraints: constraints,
            metadata: metadata,
            container: container
          };
        },

        // Given a list of instanceIds, group them into contiguous ranges.
        toRanges: function (instanceIds) {
          instanceIds = _.sortBy(instanceIds);
          var ranges = [];
          var i = 0;
          var start = instanceIds[i];
          while (i < instanceIds.length) {
            if ((i + 1 === instanceIds.length) || (instanceIds[i] + 1 !== instanceIds[i + 1])) {
              ranges.push({start: start, end: instanceIds[i]});
              i++;
              start = instanceIds[i];
            } else {
              i++;
            }
          }
          return ranges;
        },

        // A function that converts a task constraint into a string
        formatConstraint: function (constraint) {
          var taskConstraint = constraint.constraint;

          var valueConstraintStr = '';
          var valueConstraint = taskConstraint.value;
          if (valueConstraint && valueConstraint.values && _.isArray(valueConstraint.values)) {
            var values = valueConstraint.values.join(',');
            valueConstraintStr = valueConstraint.negated ? 'not ' + values : values;
          }

          var limitConstraintStr = taskConstraint.limit ? JSON.stringify(taskConstraint.limit) : '';

          if (_.isEmpty(limitConstraintStr) && _.isEmpty(valueConstraintStr)) {
            return '';
          } else {
            return constraint.name + ':' +
              (_.isEmpty(limitConstraintStr) ? valueConstraintStr : limitConstraintStr);
          }
        },

        isActiveTask: function (task) {
          return _.contains(ACTIVE_STATES, task.status);
        }
      };
      return taskUtil;
    });

  auroraUI.factory(
    'cronJobSummaryService',
    [ '$q', 'auroraClient',
      function ($q, auroraClient) {
        var cronJobSmrySvc = {
          getCronJobSummary: function (role, env, jobName) {
            var deferred = $q.defer();
            auroraClient.getJobSummary(role).then(function (summaries) {
              if (summaries.error) {
                deferred.resolve({error: 'Failed to fetch cron schedule from scheduler.'});
              }

              var cronJobSummary = _.chain(summaries.jobs)
                .filter(function (summary) {
                  // fetch the cron job with a matching name and env.
                  var job = summary.job;
                  return job.cronSchedule !== null &&
                    job.key.environment === env &&
                    job.taskConfig.jobName === jobName;
                })
                .map(function (summary) {
                  var collisionPolicy =
                    cronJobSmrySvc.getCronCollisionPolicy(summary.job.cronCollisionPolicy);
                  // summarize the cron job.
                  return {
                    tasks: summary.job.instanceCount,
                    schedule: summary.job.cronSchedule,
                    nextCronRun: summary.nextCronRunMs,
                    collisionPolicy: collisionPolicy,
                    metadata: cronJobSmrySvc.getMetadata(summary.job.taskConfig.metadata)
                  };
                })
                .last() // there will always be 1 job in this list.
                .value();

              deferred.resolve({error: '', cronJobSummary: cronJobSummary});
            });
            return deferred.promise;
          },

          getMetadata: function (attributes) {
            return _.map(attributes, function (attribute) {
              return attribute.key + ': ' + attribute.value;
            }).join(', ');
          },

          getCronCollisionPolicy: function (cronCollisionPolicy) {
            return _.keys(CronCollisionPolicy)[cronCollisionPolicy ? cronCollisionPolicy : 0];
          }
        };
        return cronJobSmrySvc;
      }]);

  auroraUI.factory(
    'jobTasksService',
    ['auroraClient', 'taskUtil', function jobTasksServiceFactory(auroraClient, taskUtil) {
      var baseTableConfig = {
        isGlobalSearchActivated: false,
        isPaginationEnabled: true,
        itemsByPage: 50,
        maxSize: 8,
        selectionMode: 'single'
      };

      function addColumn(afterLabel, currentColumns, newColumn) {
        var idxPosition = -1;
        currentColumns.some(function (column, index) {
          if (column.label === afterLabel) {
            idxPosition = index + 1;
            return true;
          }

          return false;
        });

        if (idxPosition === -1) {
          return;
        }

        return _.union(
          _.first(currentColumns, idxPosition),
          [newColumn],
          _.last(currentColumns, currentColumns.length - idxPosition));
      }

      var baseColumns = [
        {label: 'Instance', map: 'instanceId', cellTemplateUrl: '/assets/taskInstance.html'},
        {label: 'Status', map: 'status', cellTemplateUrl: '/assets/taskStatus.html'},
        {label: 'Host', map: 'host', cellTemplateUrl: '/assets/taskSandbox.html'}
      ];

      var completedTaskColumns = addColumn(
        'Status',
        baseColumns,
        {
          label: 'Running duration',
          map: 'duration',
          formatFunction: function (duration) {
            return moment.duration(duration).humanize();
          }
        });

      function summarizeTask(task) {
        var isActive = taskUtil.isActiveTask(task);
        var sortedTaskEvents = _.sortBy(task.taskEvents, function (taskEvent) {
          return taskEvent.timestamp;
        });

        var latestTaskEvent = _.last(sortedTaskEvents);

        return {
          instanceId: task.assignedTask.instanceId,
          jobKey: task.assignedTask.task.job,
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

      function getJobDashboardUrl(statsUrlPrefix, role, environment, job) {
        return _.isEmpty(statsUrlPrefix) ?
          '' :
          statsUrlPrefix + role + '.' + environment + '.' + job;
      }

      function clone(obj) {
        return JSON.parse(JSON.stringify(obj));
      }

      return {
        taskIdColumn:  {
          label: 'Task ID',
          map: 'taskId',
          cellTemplateUrl: '/assets/taskLink.html'
        },

        taskColumns: baseColumns,
        completedTaskColumns: completedTaskColumns,
        addColumn: addColumn,

        getTasksForJob: function getTasksForJob($scope) {
          $scope.activeTasksTableColumns = baseColumns;
          $scope.completedTasksTableColumns = completedTaskColumns;

          $scope.activeTasksTableConfig = clone(baseTableConfig);
          $scope.completedTasksTableConfig = clone(baseTableConfig);

          auroraClient.getTasksWithoutConfigs(
              $scope.role,
              $scope.environment,
              $scope.job,
              $scope.instance)
            .then(function (response) {
              if (response.error) {
                $scope.error = 'Error fetching tasks: ' + response.error;
                return [];
              }

              $scope.jobDashboardUrl = getJobDashboardUrl(
                response.statsUrlPrefix,
                $scope.role,
                $scope.environment,
                $scope.job);

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

              $scope.activeTasksTableConfig.isPaginationEnabled =
                  $scope.activeTasks.length > $scope.activeTasksTableConfig.itemsByPage;
              $scope.completedTasksTableConfig.isPaginationEnabled =
                  $scope.completedTasks.length > $scope.completedTasksTableConfig.itemsByPage;

              $scope.tasksReady = true;
            });
        }
      };
    }]);
})();
