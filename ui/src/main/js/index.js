import React from 'react';
import ReactDOM from 'react-dom';
import { BrowserRouter as Router, Route } from 'react-router-dom';

import Home from 'components/Home';

const SchedulerUI = () => (
  <Router>
    <div>
      <Route component={Home} exact path='/beta/scheduler' />
      <Route component={Home} exact path='/beta/scheduler/:role' />
      <Route component={Home} exact path='/beta/scheduler/:role/:environment' />
      <Route component={Home} exact path='/beta/scheduler/:role/:environment/:name' />
      <Route component={Home} exact path='/beta/scheduler/:role/:environment/:name/:instance' />
      <Route component={Home} exact path='/beta/scheduler/:role/:environment/:name/update/:uid' />
      <Route component={Home} exact path='/beta/updates' />
    </div>
  </Router>
);

ReactDOM.render(<SchedulerUI />, document.getElementById('root'));
