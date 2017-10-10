import React from 'react';
import ReactDOM from 'react-dom';
import { BrowserRouter as Router, Route } from 'react-router-dom';

import SchedulerClient from 'client/scheduler-client';
import Navigation from 'components/Navigation';
import Home from 'pages/Home';
import Instance from 'pages/Instance';
import Jobs from 'pages/Jobs';

import styles from '../sass/app.scss'; // eslint-disable-line no-unused-vars

const injectApi = (Page) => (props) => <Page api={SchedulerClient} {...props} />;

const SchedulerUI = () => (
  <Router>
    <div>
      <Navigation />
      <Route component={injectApi(Home)} exact path='/beta/scheduler' />
      <Route component={injectApi(Jobs)} exact path='/beta/scheduler/:role' />
      <Route component={injectApi(Jobs)} exact path='/beta/scheduler/:role/:environment' />
      <Route component={Home} exact path='/beta/scheduler/:role/:environment/:name' />
      <Route
        component={injectApi(Instance)}
        exact
        path='/beta/scheduler/:role/:environment/:name/:instance' />
      <Route component={Home} exact path='/beta/scheduler/:role/:environment/:name/update/:uid' />
      <Route component={Home} exact path='/beta/updates' />
    </div>
  </Router>
);

ReactDOM.render(<SchedulerUI />, document.getElementById('root'));
