import React from 'react';
import { shallow } from 'enzyme';

import { Link } from 'react-router-dom';

import Breadcrumb from '../Breadcrumb';

describe('Breadcrumb', () => {
  it('Should render cluster crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' />);
    expect(el.contains(<Link to='/beta/scheduler'>devcluster</Link>)).toBe(true);
    expect(el.find(Link).length).toBe(1);
  });

  it('Should render role crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' role='www-data' />);
    expect(el.contains(<Link to='/beta/scheduler/www-data'>www-data</Link>)).toBe(true);
    expect(el.find(Link).length).toBe(2);
  });

  it('Should render env crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' env='prod' role='www-data' />);
    expect(el.contains(<Link to='/beta/scheduler/www-data/prod'>prod</Link>)).toBe(true);
    expect(el.find(Link).length).toBe(3);
  });

  it('Should render name crumb', () => {
    const el = shallow(<Breadcrumb cluster='devcluster' env='prod' name='hello' role='www-data' />);
    expect(el.contains(<Link to='/beta/scheduler/www-data/prod/hello'>hello</Link>)).toBe(true);
    expect(el.find(Link).length).toBe(4);
  });
});
