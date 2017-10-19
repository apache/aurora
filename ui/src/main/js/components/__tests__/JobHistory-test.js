import React from 'react';
import { shallow } from 'enzyme';

import JobHistory from '../JobHistory';
import TaskList from '../TaskList';

import { ScheduledTaskBuilder } from 'test-utils/TaskBuilders';

describe('JobHistory', () => {
  it('Should render a task list only with terminal tasks', () => {
    const tasks = [
      ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
      ScheduledTaskBuilder.status(ScheduleStatus.FINISHED).build()
    ];
    const el = shallow(JobHistory({tasks}));
    expect(el.contains(<TaskList tasks={[tasks[1]]} />)).toBe(true);
  });
});
