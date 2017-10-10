import React from 'react';
import { shallow } from 'enzyme';

import Loading from '../Loading';
import UpdateList from '../UpdateList';

describe('UpdateList', () => {
  it('Handles null by showing Loading', () => {
    const el = shallow(<UpdateList />);
    expect(el.contains(<Loading />)).toBe(true);
  });

  it('Does not show loading when data is passed, even an empty list', () => {
    const el = shallow(<UpdateList updates={[]} />);
    expect(el.contains(<Loading />)).toBe(false);
  });
});
