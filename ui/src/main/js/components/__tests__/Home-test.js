import React from 'react';
import { shallow } from 'enzyme';

import Home from '../Home';

describe('Home', () => {
  it('Should render Hello, World!', () => {
    expect(shallow(<Home />).equals(<div>Hello, World!</div>)).toBe(true);
  });
});
