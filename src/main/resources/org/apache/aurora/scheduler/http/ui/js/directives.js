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
      templateUrl: '/breadcrumb.html'
    };
  });

  auroraUI.directive('error', function () {
    return {
      restrict: 'E',
      templateUrl: '/error.html'
    };
  });

  auroraUI.directive('taskSandboxLink', function () {
    return {
      restrict: 'E',
      templateUrl: '/taskSandbox.html'
    };
  });

  auroraUI.directive('taskStatus', function () {
    return {
      restrict: 'E',
      replace: true,
      link: function (scope, element, attrs, ctrl) {
        element.on('click', function (e) {
          scope.showDetails = !scope.showDetails;
        });
      }
    };
  });

  auroraUI.directive('taskLink', function () {
    return {
      restrict: 'C',
      template: '<a class="span4" ng-href="/structdump/task/{{formatedValue}}" target="_self">' +
        '{{formatedValue}}</a>'
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
      templateUrl: '/groupSummary.html',
      scope: {
        'groups': '=',
        'visibleGroups': '=?'
      },
      replace: true,
      link: function (scope) {
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
      }
    };
  });

  auroraUI.directive('configSummary', function () {
    return {
      restrict: 'E',
      scope: {
        'group': '='
      },
      templateUrl: '/configSummary.html',
      replace: true
    };
  });
})();