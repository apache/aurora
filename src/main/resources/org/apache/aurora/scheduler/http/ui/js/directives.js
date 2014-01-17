'use strict';

auroraUI.directive('roleLink', function () {
  return {
    restrict: 'C',
    template: "<a ng-href='/scheduler/{{formatedValue}}'>{{formatedValue}}</a>"
  };
});