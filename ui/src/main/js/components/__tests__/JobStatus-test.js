import React from 'react';
import { shallow } from 'enzyme';

import CronJobPreview from '../CronJobPreview';
import JobStatus from '../JobStatus';
import TaskList from '../TaskList';
import { Tab } from '../Tabs';

import {
  ScheduledTaskBuilder,
  TaskConfigBuilder,
  createConfigGroup
} from 'test-utils/TaskBuilders';

describe('JobStatus', () => {
  it('Should render a task list only with active tasks', () => {
    const tasks = [
      ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
      ScheduledTaskBuilder.status(ScheduleStatus.FINISHED).build()
    ];
    const el = shallow(JobStatus({queryParams: {}, tasks}));
    expect(el.contains(<TaskList tasks={[tasks[0]]} />)).toBe(true);
  });

  it('Should render the correct number of task configs', () => {
    const configGroups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder, [1, 10])
    ];
    const el = shallow(JobStatus({configGroups, queryParams: {}, tasks: []}));
    expect(el.find(Tab).someWhere((t) => t.props().name === 'Configuration (2)')).toBe(true);
  });

  it('Should show one configuration when there is a cron job', () => {
    const cronJob = {};
    const el = shallow(JobStatus({cronJob: cronJob, queryParams: {}, tasks: []}));
    expect(el.contains(<CronJobPreview cronJob={cronJob} />)).toBe(true);
    expect(el.find(Tab).someWhere((t) => t.props().name === 'Configuration (1)')).toBe(true);
  });
});
