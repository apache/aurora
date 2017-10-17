import React from 'react';
import { shallow } from 'enzyme';

import Diff from '../Diff';

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
});
