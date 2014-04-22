'use strict';

// Declare app level module which depends on filters, and services
var auroraUI = angular.module('auroraUI', ['ngRoute', 'auroraUI.controllers', 'smartTable.table']);

auroraUI.config(function ($routeProvider, $locationProvider) {
  $routeProvider.when("/scheduler",
    {templateUrl: '/home.html', controller: 'RoleSummaryController'});

  $routeProvider.when("/scheduler/:role",
    {templateUrl: '/role.html', controller: 'JobSummaryController'});

  $routeProvider.when("/scheduler/:role/:environment",
    {templateUrl: '/role.html', controller: 'JobSummaryController'});

  $routeProvider.when("/scheduler/:role/:environment/:job",
    {templateUrl: '/job.html', controller: 'JobController'});

  $routeProvider.otherwise({redirectTo: function (location, path) {
    window.location.href = path;
  }});

  $locationProvider.html5Mode(true);
});
