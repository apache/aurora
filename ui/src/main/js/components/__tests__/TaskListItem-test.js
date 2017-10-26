import React from 'react';
import { shallow } from 'enzyme';

import TaskListItem from '../TaskListItem';
import TaskStateMachine from '../TaskStateMachine';

import { ScheduledTaskBuilder } from 'test-utils/TaskBuilders';

describe('TaskListItem', () => {
  it('Should not show any state machine element by default', () => {
    const el = shallow(<TaskListItem task={ScheduledTaskBuilder.build()} />);
    expect(el.find(TaskStateMachine).length).toBe(0);
    expect(el.find('tr.expanded').length).toBe(0);
  });

  it('Should show the state machine and add expanded to row when expand link is clicked', () => {
    const el = shallow(<TaskListItem task={ScheduledTaskBuilder.build()} />);
    el.find('.task-list-item-expander').simulate('click');
    expect(el.find(TaskStateMachine).length).toBe(1);
    expect(el.find('tr.expanded').length).toBe(1);
  });
});
