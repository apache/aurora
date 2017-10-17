import React from 'react';
import { shallow } from 'enzyme';

import Diff from '../Diff';
import ConfigDiff from '../ConfigDiff';

import { TaskConfigBuilder, createConfigGroup } from 'test-utils/TaskBuilders';

describe('ConfigDiff', () => {
  it('Should render an empty div when there are less than 2 config groups', () => {
    const groups = [createConfigGroup(TaskConfigBuilder, [0, 0])];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.contains(<div />)).toBe(true);
  });

  it('Should default to showing a diff of the first two configs when multiple are provided', () => {
    const groups = [
      createConfigGroup(TaskConfigBuilder, [0, 0]),
      createConfigGroup(TaskConfigBuilder, [1, 9]),
      createConfigGroup(TaskConfigBuilder, [10, 20])
    ];
    const el = shallow(<ConfigDiff groups={groups} />);
    expect(el.contains(<Diff left={groups[0].config} right={groups[1].config} />)).toBe(true);
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
    expect(el.contains(<Diff left={groups[0].config} right={groups[1].config} />)).toBe(true);

    el.find('.diff-before select').simulate('change', {target: {value: '2'}});
    expect(el.contains(<Diff left={groups[2].config} right={groups[1].config} />)).toBe(true);
  });
});
