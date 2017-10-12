import React from 'react';
import { shallow } from 'enzyme';

import ConfigDiff from '../ConfigDiff';
import JobConfig from '../JobConfig';
import Loading from '../Loading';
import TaskConfigSummary from '../TaskConfigSummary';

import { TaskConfigBuilder, createConfigGroup } from 'test-utils/TaskBuilders';

describe('JobConfig', () => {
  it('Should render summaries and diff with configs in order of lowest instance id', () => {
    const group0 = createConfigGroup(TaskConfigBuilder, [0, 0]);
    const group1 = createConfigGroup(TaskConfigBuilder, [1, 9]);
    const group2 = createConfigGroup(TaskConfigBuilder, [10, 10]);

    const el = shallow(<JobConfig groups={[group2, group0, group1]} />);
    const summaries = el.find(TaskConfigSummary).map((i) => i.props().instances);
    expect(summaries).toEqual([group0.instances, group1.instances, group2.instances]);
    expect(el.contains(<ConfigDiff groups={[group0, group1, group2]} />)).toBe(true);
  });

  it('Should render Loading when no groups are supplied', () => {
    const el = shallow(<JobConfig />);
    expect(el.contains(<Loading />)).toBe(true);
  });
});
