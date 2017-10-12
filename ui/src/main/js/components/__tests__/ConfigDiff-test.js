import React from 'react';
import { shallow } from 'enzyme';

import ConfigDiff from '../ConfigDiff';

import { TaskConfigBuilder, createConfigGroup } from 'test-utils/TaskBuilders';

describe('ConfigDiff', () => {
  it('Should render an empty div when there are less than 2 config groups', () => {
    const groups = [createConfigGroup(TaskConfigBuilder, [0, 0])];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.contains(<div />)).toBe(true);
  });

  it('Should not add change classes to diff viewer when configs are same', () => {
    const groups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder, [1, 9])
    ];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.find('span.removed').length).toBe(0);
    expect(el.find('span.added').length).toBe(0);
  });

  it('Should add change classes to diff viewer when configs are not the same', () => {
    const groups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder.tier('something-else'), [1, 9])
    ];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.find('span.removed').length).toBe(1);
    expect(el.find('span.added').length).toBe(1);
  });

  it('Should not show any config group dropdown when there are only two groups', () => {
    const groups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder.tier('something-else'), [1, 9])
    ];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.find('select').length).toBe(0);
  });

  it('Should show a group dropdown when there are more than two groups', () => {
    const groups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder.tier('something-else'), [1, 1]),
      createConfigGroup(TaskConfigBuilder.tier('something-else'), [2, 2])
    ];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.find('select').length).toBe(2);
    expect(el.find('option').length).toBe(4);
  });

  it('Should update the diff view when you select new groups', () => {
    const groups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder.tier('something-else'), [1, 1]),
      createConfigGroup(TaskConfigBuilder.tier('something-else'), [2, 2])
    ];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.find('span.removed').length).toBe(1);
    expect(el.find('span.added').length).toBe(1);

    expect(el.find('.diff-before select').length).toBe(1);
    expect(el.find('option').length).toBe(4);

    // Change the left config to be index=2, which has the same config as index=1
    el.find('.diff-before select').simulate('change', {target: {value: '2'}});
    // Now assert the diff was updated!
    expect(el.find('span.removed').length).toBe(0);
    expect(el.find('span.added').length).toBe(0);
    expect(el.find('option').length).toBe(4);
  });
});
