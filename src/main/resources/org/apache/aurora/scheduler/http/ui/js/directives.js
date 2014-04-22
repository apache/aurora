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
    template: '<a ng-href="/scheduler/{{dataRow.role}}/{{dataRow.environment}}/{{formatedValue}}">'
      + '{{formatedValue}}</a>'
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
})
