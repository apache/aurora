import React from 'react';
import { shallow } from 'enzyme';

import TaskDetails from '../TaskDetails';

import { AssignedTaskBuilder, ScheduledTaskBuilder } from 'test-utils/TaskBuilders';

describe('TaskDetails', () => {
  it('Does not show host when slaveHost is null', () => {
    const task = ScheduledTaskBuilder.assignedTask(
      AssignedTaskBuilder.slaveHost(null).build()).build();
    const el = shallow(<TaskDetails task={task} />);

    expect(el.find('div.active-task-details-host').length).toBe(0);
  });

  it('Shows host link when slaveHost is present', () => {
    const task = ScheduledTaskBuilder.assignedTask(
      AssignedTaskBuilder.slaveHost('test').build()).build();
    const el = shallow(<TaskDetails task={task} />);

    expect(el.find('div.active-task-details-host').length).toBe(1);
  });
});
