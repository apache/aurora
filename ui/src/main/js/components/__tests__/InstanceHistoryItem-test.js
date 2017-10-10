import React from 'react';
import { shallow } from 'enzyme';

import InstanceHistoryItem, {
  InstanceHistoryBody,
  InstanceHistoryHeader
} from '../InstanceHistoryItem';

import { ScheduledTaskBuilder, TaskEventBuilder } from 'test-utils/TaskBuilders';

describe('InstanceHistoryItem', () => {
  it('Should be minimized by default', () => {
    const task = ScheduledTaskBuilder.build();
    const el = shallow(<InstanceHistoryItem task={task} />);
    expect(el.find(InstanceHistoryHeader).length).toBe(1);
    expect(el.find(InstanceHistoryBody).length).toBe(0);
  });

  it('Should render body when expanded', () => {
    const task = ScheduledTaskBuilder.build();
    const el = shallow(<InstanceHistoryItem expanded task={task} />);
    expect(el.find(InstanceHistoryHeader).length).toBe(1);
    expect(el.find(InstanceHistoryBody).length).toBe(1);
  });
});

describe('InstanceHistoryHeader', () => {
  it('Should call toggle when the item details is clicked', () => {
    const task = ScheduledTaskBuilder.build();
    const mockFn = jest.fn();
    const el = shallow(<InstanceHistoryHeader task={task} toggle={mockFn} />);
    el.find('.instance-history-item-details').simulate('click');
    expect(mockFn.mock.calls.length).toBe(1);
  });

  it('Should render the attention status icon for pending tasks', () => {
    const task = ScheduledTaskBuilder.status(ScheduleStatus.PENDING).build();
    const el = shallow(<InstanceHistoryHeader task={task} toggle={jest.fn()} />);
    expect(el.find('.img-circle.attention').length).toBe(1);
  });

  it('Should render the okay status icon for finished tasks', () => {
    const task = ScheduledTaskBuilder.status(ScheduleStatus.FINISHED).build();
    const el = shallow(<InstanceHistoryHeader task={task} toggle={jest.fn()} />);
    expect(el.find('.img-circle.okay').length).toBe(1);
  });

  it('Should render the error status icon for failed tasks', () => {
    const task = ScheduledTaskBuilder.status(ScheduleStatus.FAILED).build();
    const el = shallow(<InstanceHistoryHeader task={task} toggle={jest.fn()} />);
    expect(el.find('.img-circle.error').length).toBe(1);
  });

  it('Should render the correct duration', () => {
    const task = ScheduledTaskBuilder.taskEvents([
      TaskEventBuilder.timestamp(0).build(),
      TaskEventBuilder.timestamp(10).build(),
      TaskEventBuilder.timestamp(60000).build()
    ]).build();
    const el = shallow(<InstanceHistoryHeader task={task} toggle={jest.fn()} />);
    expect(el.contains(<span>Running duration: {'a minute'}</span>)).toBe(true);
  });
});
