import React from 'react';
import ReactDOM from 'react-dom';
import { BrowserRouter as Router, Route } from 'react-router-dom';

import SchedulerClient from 'client/scheduler-client';
import Navigation from 'components/Navigation';
import Home from 'pages/Home';
import Instance from 'pages/Instance';
import Job from 'pages/Job';
import Jobs from 'pages/Jobs';
import Update from 'pages/Update';
import Updates from 'pages/Updates';

import 'bootstrap/dist/css/bootstrap.css';
import '../resources/source-sans-pro.css'
import '../sass/app.scss';

const injectApi = (Page) => (props) => <Page api={SchedulerClient} {...props} />;

const SchedulerUI = () => (
  <Router>
    <div>
      <Navigation />
      <Route component={injectApi(Home)} exact path='/beta/scheduler' />
      <Route component={injectApi(Jobs)} exact path='/beta/scheduler/:role' />
      <Route component={injectApi(Jobs)} exact path='/beta/scheduler/:role/:environment' />
      <Route component={injectApi(Job)} exact path='/beta/scheduler/:role/:environment/:name' />
      <Route
        component={injectApi(Instance)}
        exact
        path='/beta/scheduler/:role/:environment/:name/:instance' />
      <Route
        component={injectApi(Update)}
        exact
        path='/beta/scheduler/:role/:environment/:name/update/:uid' />
      <Route component={injectApi(Updates)} exact path='/beta/updates' />
    </div>
  </Router>
);

ReactDOM.render(<SchedulerUI />, document.getElementById('root'));
