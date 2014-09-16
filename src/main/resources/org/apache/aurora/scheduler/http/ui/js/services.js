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
  /* global auroraUI:false, Identity:false, TaskQuery:false, ReadOnlySchedulerClient:false,
            ACTIVE_STATES:false, CronCollisionPolicy: false, JobKey: false,
            ScheduleStatus: false, JobUpdateQuery:false, JobUpdateAction:false,
            JobUpdateStatus: false */
  'use strict';

  function makeJobTaskQuery(role, environment, jobName) {
    var id = new Identity();
    id.role = role;
    var taskQuery = new TaskQuery();
    taskQuery.owner = id;
    taskQuery.environment = environment;
    taskQuery.jobName = jobName;
    return taskQuery;
  }

  auroraUI.factory(
    'auroraClient',
    ['$window',
      function ($window) {
        var auroraClient = {
          // Each of the functions below wrap an API call on the scheduler.
          getRoleSummary: function () {
            var response = auroraClient.getSchedulerClient().getRoleSummary();
            var result = auroraClient.processResponse(response);
            result.summaries = response.result !== null ?
              response.result.roleSummaryResult.summaries : [];
            return result;
          },

          getJobSummary: function (role) {
            var response = auroraClient.getSchedulerClient().getJobSummary(role);
            var result = auroraClient.processResponse(response);
            result.jobs = response.result !== null ?
              response.result.jobSummaryResult.summaries : [];
            return result;
          },

          getQuota: function (role) {
            var response = auroraClient.getSchedulerClient().getQuota(role);
            var result = auroraClient.processResponse(response);
            result.quota = response.result !== null ? response.result.getQuotaResult : [];
            return result;
          },

          getTasks: function (role, environment, jobName) {
            var id = new Identity();
            id.role = role;
            var taskQuery = new TaskQuery();
            taskQuery.owner = id;
            taskQuery.environment = environment;
            taskQuery.jobName = jobName;
            var response = auroraClient.getSchedulerClient().getTasksStatus(taskQuery);
            var result = auroraClient.processResponse(response);
            result.tasks = response.result !== null ?
              response.result.scheduleStatusResult.tasks : [];
            return result;
          },

          getTasksWithoutConfigs: function (role, environment, jobName) {
            var query = makeJobTaskQuery(role, environment, jobName);
            var response = auroraClient.getSchedulerClient().getTasksWithoutConfigs(query);
            var result = auroraClient.processResponse(response);
            result.tasks = response.result !== null ?
              response.result.scheduleStatusResult.tasks : [];

            // Attach current pending reasons to any pending tasks we might have
            var pendingTasks = _.filter(result.tasks, function (t) {
              return t.status === ScheduleStatus.PENDING;
            });

            if (pendingTasks.length > 0) {
              var pendingResponse = auroraClient.getSchedulerClient().getPendingReason(query);
              var reasons = pendingResponse.result !== null ?
                pendingResponse.result.getPendingReasonResult.reasons : [];

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

            return result;
          },

          getConfigSummary: function (role, environment, jobName) {
            var key = new JobKey();
            key.role = role;
            key.environment = environment;
            key.name = jobName;
            var response = auroraClient.getSchedulerClient().getConfigSummary(key);
            var result = auroraClient.processResponse(response);
            result.groups = response.result !== null ?
              response.result.configSummaryResult.summary.groups : [];
            return result;
          },

          getJobUpdateSummaries: function (query) {
            query = query || new JobUpdateQuery();
            var response = auroraClient.getSchedulerClient().getJobUpdateSummaries(query);
            var result = auroraClient.processResponse(response);
            result.summaries = response.result !== null ?
              response.result.getJobUpdateSummariesResult.updateSummaries : [];
            return result;
          },

          getJobUpdateDetails: function (id) {
            var response = auroraClient.getSchedulerClient().getJobUpdateDetails(id);
            var result = auroraClient.processResponse(response);
            result.details = response.result !== null ?
              response.result.getJobUpdateDetailsResult.details : {};
            return result;
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
            var title = 'Aurora UI';
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

      var UPDATE_TERMINAL = toSet([
        JobUpdateStatus.ROLLED_FORWARD,
        JobUpdateStatus.ROLLED_BACK,
        JobUpdateStatus.ABORTED,
        JobUpdateStatus.ERROR
      ]);

      var INSTANCE_SUCCESSFUL = toSet([
        JobUpdateAction.INSTANCE_UPDATED,
        JobUpdateAction.INSTANCE_REMOVED
      ]);

      var INSTANCE_TERMINAL = toSet([
        JobUpdateAction.INSTANCE_UPDATED,
        JobUpdateAction.INSTANCE_REMOVED,
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

          // total number of instances to show is the max between
          // new instance count and old instance count
          var totalInstances = Math.max(
              stats.oldInstanceCount,
              details.update.configuration.instanceCount
            );

          var instances = [];
          var instanceSubset = details.update.configuration.settings.updateOnlyTheseInstances;

          function inRanges(ranges, x) {
            if (ranges && x) {
              for (var i = 0; i < ranges.length; i++) {
                if (x >= ranges[i].first && x <= ranges[i].last) {
                  return true;
                }
              }
            }
            return false;
          }

          for (var i = 0; i < totalInstances; i++) {
            if (instanceMap.hasOwnProperty(i)) {
              var event = instanceMap[i];
              var className = updateUtil.displayClassForInstanceStatus(event.action);
              instances.push({
                instanceId: i,
                className: className,
                event: event
              });
            } else if (instanceSubset && !inRanges(instanceSubset, i)) {
              // If they have declared a subset of instances to update
              // AND this instance isn't part of that subset, it will be ignored.
              instances.push({
                instanceId: i,
                className: 'ignore'
              });
            } else {
              // Otherwise it is pending an update.
              instances.push({
                instanceId: i,
                className: 'pending'
              });
            }
          }
          return instances;
        },
        getUpdateStats: function (details) {
          if (!details || !details.update) {
            return {};
          }

          // find number of instances to be updated
          var newInstanceCount = details.update.configuration.instanceCount;
          var updateSubset = false;

          // find total number of existing instances
          var oldInstanceCount = updateUtil.instanceCountFromConfigs(
            details.update.configuration.oldTaskConfigs);

          // max of those two numbers is the number of instances to be updated
          var totalInstancesToBeUpdated = Math.max(oldInstanceCount, newInstanceCount);

          if (details.update.configuration.settings.updateOnlyTheseInstances) {
            newInstanceCount = updateUtil.instanceCountFromRanges(
              details.update.configuration.settings.updateOnlyTheseInstances
            );
            updateSubset = true;
            totalInstancesToBeUpdated = newInstanceCount;
          }

          // if necessary, differentiate between number of instances updated
          // and number of instances that will be discarded
          var instancesToBeUpdated = newInstanceCount;
          var instancesToBeDiscarded = 0;
          if (!updateSubset && (oldInstanceCount > newInstanceCount)) {
            instancesToBeDiscarded = oldInstanceCount - newInstanceCount;
          }

          var instancesUpdated = updateUtil.progressFromEvents(details.instanceEvents);

          // calculate the percentage of work done so far
          var progress = Math.round((instancesUpdated / totalInstancesToBeUpdated) * 100);

          return {
            onlySubset: updateSubset,
            oldInstanceCount: oldInstanceCount,
            totalInstancesToBeUpdated: totalInstancesToBeUpdated,
            instancesToBeUpdated: instancesToBeUpdated,
            instancesToBeDiscarded: instancesToBeDiscarded,
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

          return {
            numCpus: task.numCpus,
            ramMb: task.ramMb,
            diskMb: task.diskMb,
            isService: task.isService,
            production: task.production,
            contact: task.contactEmail || '',
            ports: _.sortBy(task.requestedPorts).join(', '),
            constraints: constraints,
            metadata: metadata
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
    [ 'auroraClient',
      function (auroraClient) {
        var cronJobSmrySvc = {
          getCronJobSummary: function (role, env, jobName) {
            var summaries = auroraClient.getJobSummary(role);

            if (summaries.error) {
              return {error: 'Failed to fetch cron schedule from scheduler.'};
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

            return {error: '', cronJobSummary: cronJobSummary};
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
})();
