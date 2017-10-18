import React from 'react';
import { shallow } from 'enzyme';

import Diff from '../Diff';

import { TaskConfigBuilder } from 'test-utils/TaskBuilders';

describe('Diff', () => {
  it('Should not add change classes to diff viewer when objects are same', () => {
    const el = shallow(<Diff left={{test: true}} right={{test: true}} />);
    expect(el.find('span.removed').length).toBe(0);
    expect(el.find('span.added').length).toBe(0);
  });

  it('Should add change classes to diff viewer when configs are not the same', () => {
    const el = shallow(<Diff left={{test: true}} right={{test: false}} />);
    expect(el.find('span.removed').length).toBe(1);
    expect(el.find('span.added').length).toBe(1);
  });

  it('Should render a finer grained diff when Thermos executor is used', () => {
    const left = TaskConfigBuilder.executorConfig({
      name: 'AuroraExecutor',
      data: JSON.stringify({
        one: 1,
        two: 2,
        three: 3,
        nested: {
          okay: true
        }
      })
    }).build();

    const right = TaskConfigBuilder.executorConfig({
      name: 'AuroraExecutor',
      data: JSON.stringify({
        one: 'one',
        two: 2,
        three: 3,
        nested: {
          okay: false
        }
      })
    }).build();

    const el = shallow(<Diff left={left} right={right} />);
    expect(el.find('span.removed').length).toBe(2);
    expect(el.find('span.added').length).toBe(2);
  });

  it('Just treats executor config as a string for custom executors', () => {
    const left = TaskConfigBuilder.executorConfig({
      name: 'MyExecutor',
      data: JSON.stringify({
        one: 1,
        two: 2,
        three: 3,
        nested: {
          okay: true
        }
      })
    }).build();

    const right = TaskConfigBuilder.executorConfig({
      name: 'MyExecutor',
      data: JSON.stringify({
        one: 'one',
        two: 2,
        three: 3,
        nested: {
          okay: false
        }
      })
    }).build();

    const el = shallow(<Diff left={left} right={right} />);
    expect(el.find('span.removed').length).toBe(1);
    expect(el.find('span.added').length).toBe(1);
  });
});
