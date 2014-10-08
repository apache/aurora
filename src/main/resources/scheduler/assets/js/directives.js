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
  /*global auroraUI:false */
  'use strict';

  auroraUI.directive('roleLink', function () {
    return {
      restrict: 'C',
      template: '<a ng-href="/scheduler/{{formatedValue}}">{{formatedValue}}</a>'
    };
  });

  auroraUI.directive('roleEnvLink', function () {
    return {
      restrict: 'C',
      template: '<a ng-href="/scheduler/{{dataRow.role}}/{{formatedValue}}">{{formatedValue}}</a>'
    };
  });

  auroraUI.directive('jobLink', function () {
    return {
      restrict: 'C',
      template:
        '<a ng-href="/scheduler/{{dataRow.role}}/{{dataRow.environment}}/{{formatedValue}}">' +
        '{{formatedValue}}</a>'
    };
  });

  auroraUI.directive('breadcrumb', function () {
    return {
      restrict: 'E',
      templateUrl: '/assets/breadcrumb.html'
    };
  });

  auroraUI.directive('error', function () {
    return {
      restrict: 'E',
      templateUrl: '/assets/error.html'
    };
  });

  auroraUI.directive('taskSandboxLink', function () {
    return {
      restrict: 'E',
      templateUrl: '/assets/taskSandbox.html'
    };
  });

  auroraUI.directive('taskStatus', function () {
    return {
      restrict: 'E',
      replace: true,
      link: function (scope) {
        scope.toggleShowDetails = function () {
          scope.showDetails = !scope.showDetails;
        };
      }
    };
  });

  auroraUI.directive('taskLink', function () {
    return {
      restrict: 'C',
      template: '<a class="col-md-8" ng-href="/structdump/task/{{formatedValue}}" ' +
        'target="_self">{{formatedValue}}</a>'
    };
  });

  auroraUI.directive('schedulingDetail', function () {
    return {
      restrict: 'C'
    };
  });

  auroraUI.directive('groupSummary', function () {
    return {
      restrict: 'E',
      templateUrl: '/assets/groupSummary.html',
      scope: {
        'groups': '=',
        'visibleGroups': '=?'
      },
      replace: true,
      link: function (scope) {
        scope.$watch('visibleGroups', function () {
          scope.visibleGroups = scope.visibleGroups || [];

          scope.toggleVisibleGroup = function (index) {
            var i = _.indexOf(scope.visibleGroups, index, true);
            if (i > -1) {
              scope.visibleGroups.splice(i, 1);
            } else {
              scope.visibleGroups.push(index);
              scope.visibleGroups.sort();
            }
          };

          scope.showAllGroups = function () {
            scope.visibleGroups = _.range(scope.groups.length);
          };

          scope.hideAllGroups = function () {
            scope.visibleGroups = [];
          };
        });
      }
    };
  });

  auroraUI.directive('configSummary', function () {
    return {
      restrict: 'E',
      scope: {
        'group': '='
      },
      templateUrl: '/assets/configSummary.html',
      replace: true
    };
  });

  auroraUI.directive('timeDisplay', function () {
    return {
      restrict: 'E',
      scope: {
        'timestamp': '='
      },
      templateUrl: '/assets/timeDisplay.html'
    };
  });

  auroraUI.directive('updateSettings', function () {
    return {
      restrict: 'E',
      scope: {
        'update': '='
      },
      templateUrl: '/assets/updateSettings.html'
    };
  });

  auroraUI.directive('instanceSummary', function ($compile) {
    return {
      restrict: 'E',
      scope: {
        'instances': '=',
        'size': '=',
        'stats': '='
      },
      link: function (scope, element, attrs) {
        scope.$watch('instances', function () {
          var parent = angular.element('<div></div>');
          if (!scope.instances || scope.instances.length === 0) {
            return;
          }
          var list = angular.element('<ul class="instance-grid ' + scope.size + '"></ul>');

          scope.instances.forEach(function (i) {
            var n = i.instanceId;
            list.append('<li class="' + i.className + '" tooltip="INSTANCE ' + n +
              ': ' + i.className.toUpperCase() + '"><span class="instance-id">' + n +
              '</span></li>');
          });

          var title = angular.element('<div class="instance-summary-title"></div>');
          title.append('<span class="instance-title">Instance Status</span>');
          title.append('<span class="instance-progress">' + scope.stats.instancesUpdatedSoFar +
            ' / ' + scope.stats.totalInstancesToBeUpdated + ' (' + scope.stats.progress +
            '%)<div>');

          parent.append(title);
          parent.append(list);
          element.html(parent.html());
          $compile(element)(scope);
        });
      }
    };
  });

  auroraUI.directive('latestUpdates', function () {
    return {
      restrict: 'E',
      scope: {
        'updates': '=',
        'message': '@'
      },
      templateUrl: '/assets/latestUpdates.html'
    };
  });
})();
