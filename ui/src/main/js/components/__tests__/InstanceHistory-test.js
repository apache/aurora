import React from 'react';
import { shallow } from 'enzyme';

import InstanceHistory from '../InstanceHistory';
import InstanceHistoryItem from '../InstanceHistoryItem';

describe('InstanceHistory', () => {
  it('Should reverse sort the tasks given to it by latest timestamp', () => {
    const tasks = [
      {assignedTask: {taskId: 0}, taskEvents: [{timestamp: 2}]},
      {assignedTask: {taskId: 1}, taskEvents: [{timestamp: 1}, {timestamp: 10}]},
      {assignedTask: {taskId: 2}, taskEvents: [{timestamp: 3}]}
    ];

    const el = shallow(<InstanceHistory tasks={tasks} />);
    const ids = el.find(InstanceHistoryItem).map((i) => i.props().task.assignedTask.taskId);
    expect(ids).toEqual([1, 2, 0]);
  });

  it('Should handle empty lists', () => {
    const el = shallow(<InstanceHistory tasks={[]} />);
    expect(el.contains(<div />)).toBe(true);
  });

  it('Should handle undefined tasks', () => {
    const el = shallow(<InstanceHistory />);
    expect(el.contains(<div />)).toBe(true);
  });
});
