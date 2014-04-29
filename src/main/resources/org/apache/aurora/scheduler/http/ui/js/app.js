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
