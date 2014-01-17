'use strict';

/* Controllers */

angular.module('auroraUI.controllers', []).
    controller('JobSummaryController',
    function ($scope, $window, auroraClient) {
      $scope.title = 'Scheduled Jobs Summary';

      $scope.columnCollection = [
        {label : 'Role', map: 'role', cellTemplateUrl: 'roleLink.html'},
        {label : 'Jobs', map: 'jobCount'},
        {label : 'Cron Jobs', map: 'cronJobCount'}
      ];

      $scope.rowCollection = auroraClient.getJobSummary().summaries;

      $scope.globalConfig = {
        isGlobalSearchActivated: true,
        isPaginationEnabled: true,
        itemsByPage: 25,
        maxSize: 8
      };
   });
