import React from 'react';
import { shallow } from 'enzyme';

import Diff from '../Diff';
import UpdateDiff from '../UpdateDiff';

import { TaskConfigBuilder } from 'test-utils/TaskBuilders';
import {
  UpdateBuilder,
  UpdateDetailsBuilder,
  UpdateInstructionsBuilder,
  createInstanceTaskGroup
} from 'test-utils/UpdateBuilders';

describe('UpdateDiff', () => {
  it('Should default to the first group of initialState', () => {
    const initialState = [
      createInstanceTaskGroup(TaskConfigBuilder, [0, 0]),
      createInstanceTaskGroup(TaskConfigBuilder, [1, 9]),
      createInstanceTaskGroup(TaskConfigBuilder, [10, 20])
    ];
    const update = UpdateDetailsBuilder.update(
      UpdateBuilder.instructions(
        UpdateInstructionsBuilder.initialState(initialState).build()).build()).build();
    const el = shallow(<UpdateDiff update={update} />);
    expect(el.contains(<Diff
      left={initialState[0].task}
      right={update.update.instructions.desiredState.task} />)).toBe(true);
  });

  it('Should not show any dropdown with only one config in initialState', () => {
    const initialState = [createInstanceTaskGroup(TaskConfigBuilder, [0, 0])];
    const update = UpdateDetailsBuilder.update(
      UpdateBuilder.instructions(
        UpdateInstructionsBuilder.initialState(initialState).build()).build()).build();
    const el = shallow(<UpdateDiff update={update} />);
    expect(el.find('select').length).toBe(0);
  });

  it('Should show a dropdown when there are more than two configs in initialState', () => {
    const initialState = [
      createInstanceTaskGroup(TaskConfigBuilder, [0, 0]),
      createInstanceTaskGroup(TaskConfigBuilder, [1, 9])
    ];
    const update = UpdateDetailsBuilder.update(
      UpdateBuilder.instructions(
        UpdateInstructionsBuilder.initialState(initialState).build()).build()).build();
    const el = shallow(<UpdateDiff update={update} />);
    expect(el.find('select').length).toBe(1);
  });

  it('Should update the diff view when you select new groups', () => {
    const initialState = [
      createInstanceTaskGroup(TaskConfigBuilder, [0, 0]),
      createInstanceTaskGroup(TaskConfigBuilder, [1, 9]),
      createInstanceTaskGroup(TaskConfigBuilder, [10, 20])
    ];
    const update = UpdateDetailsBuilder.update(
      UpdateBuilder.instructions(
        UpdateInstructionsBuilder.initialState(initialState).build()).build()).build();
    const el = shallow(<UpdateDiff update={update} />);
    expect(el.contains(<Diff
      left={initialState[0].task}
      right={update.update.instructions.desiredState.task} />)).toBe(true);

    el.find('select').simulate('change', {target: {value: '2'}});
    expect(el.contains(<Diff
      left={initialState[2].task}
      right={update.update.instructions.desiredState.task} />)).toBe(true);
  });
});
