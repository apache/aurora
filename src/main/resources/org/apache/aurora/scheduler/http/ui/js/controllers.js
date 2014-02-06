'use strict';

/* Controllers */

angular.module('auroraUI.controllers', []).
    controller('JobSummaryController',
    function ($scope, $window, auroraClient) {
      $scope.title = 'Scheduled Jobs Summary';

      $scope.error = false;
      $scope.reloadMsg = "An error occurred when querying the server. Please reload this page.";
      $scope.errorMsg = "";

      $scope.columnCollection = [
        {label : 'Role', map: 'role', cellTemplateUrl: 'roleLink.html'},
        {label : 'Jobs', map: 'jobCount'},
        {label : 'Cron Jobs', map: 'cronJobCount'}
      ];

      $scope.rowCollection = parseResponse(auroraClient.getJobSummary());

      function parseResponse(response) {
        $scope.error = response.error;
        $scope.errorMsg = response.errorMsg;
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

      $scope.globalConfig = {
        isGlobalSearchActivated: true,
        isPaginationEnabled: true,
        itemsByPage: 25,
        maxSize: 8
      };
   });
