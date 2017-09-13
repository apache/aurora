import React from 'react';
import { Link } from 'react-router-dom';

import Breadcrumb from '../Breadcrumb';
import shallow from 'utils/ShallowRender';

import chai, { expect } from 'chai';
import assertJsx from 'preact-jsx-chai';
chai.use(assertJsx);

describe('Breadcrumb', () => {
  it('Should render cluster crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' />);
    expect(el.contains(<Link to='/scheduler'>devcluster</Link>)).to.be.true;
    expect(el.find(<Link />).length === 1).to.be.true;
  });

  it('Should render role crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' role='www-data' />);
    expect(el.contains(<Link to='/scheduler/www-data'>www-data</Link>)).to.be.true;
    expect(el.find(<Link />).length === 2).to.be.true;
  });

  it('Should render env crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' env='prod' role='www-data' />);
    expect(el.contains(<Link to='/scheduler/www-data/prod'>prod</Link>)).to.be.true;
    expect(el.find(<Link />).length === 3).to.be.true;
  });

  it('Should render name crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' env='prod' name='hello' role='www-data' />);
    expect(el.contains(<Link to='/scheduler/www-data/prod/hello'>hello</Link>)).to.be.true;
    expect(el.find(<Link />).length === 4).to.be.true;
  });
});
