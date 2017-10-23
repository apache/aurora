import React from 'react';
import { shallow } from 'enzyme';

import {
  TaskListControls,
  TaskListStatusFilter,
  TaskListItem,
  TaskListStatus,
  searchTask
} from '../TaskList';
import TaskStateMachine from '../TaskStateMachine';

import { AssignedTaskBuilder, ScheduledTaskBuilder } from 'test-utils/TaskBuilders';

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

describe('TaskListControls', () => {
  it('Should attach active to default list element', () => {
    const el = shallow(<TaskListControls
      currentSort='default'
      onFilter={() => {}}
      onSort={() => {}}
      tasks={[ScheduledTaskBuilder.build()]} />);

    expect(el.find('li.active').text()).toContain('instance');
  });

  it('Should attach active to latest list element', () => {
    const el = shallow(<TaskListControls
      currentSort='latest'
      onFilter={() => {}}
      onSort={() => {}}
      tasks={[ScheduledTaskBuilder.build()]} />);

    expect(el.find('li.active').text()).toContain('updated');
  });
});

describe('TaskListStatus', () => {
  it('Should not show filters for one status', () => {
    const el = shallow(<TaskListStatusFilter
      onClick={() => {}}
      tasks={[
        ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
        ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build()]} />);
    expect(
      el.contains(<div>All {2} tasks are <TaskListStatus status='PENDING' /></div>)).toBe(true);
  });

  it('Should show filters for multiple status', () => {
    const el = shallow(<TaskListStatusFilter
      onClick={() => {}}
      tasks={[
        ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build(),
        ScheduledTaskBuilder.status(ScheduleStatus.RUNNING).build()]} />);
    expect(el.find(TaskListStatus).length).toBe(2);
  });
});

describe('searchTask', () => {
  it('Should match task by status', () => {
    const el = ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build();
    expect(searchTask(el, 'RUNNING')).toBe(false);
    expect(searchTask(el, 'PENDING')).toBe(true);
    expect(searchTask(el, 'pend')).toBe(true);
  });

  it('Should match task by instanceId', () => {
    const el = ScheduledTaskBuilder.assignedTask(
      AssignedTaskBuilder.instanceId(539).build()
    ).build();
    expect(searchTask(el, '1')).toBe(false);
    expect(searchTask(el, '5')).toBe(true);
    expect(searchTask(el, '53')).toBe(true);
    expect(searchTask(el, '539')).toBe(true);
  });

  it('Should match task by slaveHost', () => {
    const el = ScheduledTaskBuilder.assignedTask(
      AssignedTaskBuilder.slaveHost('aaa-zzz-123').build()
    ).build();
    expect(searchTask(el, 'y')).toBe(false);
    expect(searchTask(el, 'aAa')).toBe(true);
    expect(searchTask(el, 'zZz')).toBe(true);
    expect(searchTask(el, '123')).toBe(true);
  });
});
