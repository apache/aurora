import React from 'react';
import Home from '../Home';

import chai, { expect } from 'chai';
import assertJsx from 'preact-jsx-chai';
chai.use(assertJsx);

describe('Home', () => {
  it('Should render Hello, World!', () => {
    expect(<Home/>).to.deep.equal(<div>Hello, World!</div>);
  });
});
