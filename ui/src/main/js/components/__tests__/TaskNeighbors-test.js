import React from 'react';
import { shallow } from 'enzyme';

import TaskNeighbors, { NeighborTaskItem } from '../TaskNeighbors';

describe('TaskNeighbors', () => {
  it('Should contain empty div if empty', () => {
    const tasks = [];
    const el = shallow(<TaskNeighbors tasks={tasks} />);
    expect(el.find(NeighborTaskItem).length).toBe(0);
    expect(el.contains(<div />)).toBe(true);
  });

  it('Should be visible if not empty', () => {
    const tasks = [
      {assignedTask: {taskId: 0}, job: {role: 'test-role', env: 'test-env', name: 'test-name'}},
      {assignedTask: {taskId: 1}, job: {role: 'test-role', env: 'test-env', name: 'test-name'}}
    ];
    const el = shallow(<TaskNeighbors tasks={tasks} />);
    expect(el.find(NeighborTaskItem).length).toBe(2);
  });
});
