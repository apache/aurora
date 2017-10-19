import React from 'react';
import { shallow } from 'enzyme';

import Tabs, { Tab } from '../Tabs';

const DummyTab = ({ number }) => <span>Hello, {number}</span>;

const tabs = [
  <Tab id='one' key={1} name='one'><DummyTab number={1} /></Tab>,
  <Tab id='two' key={2} name='two'><DummyTab number={2} /></Tab>,
  <Tab id='three' key={3} name='three'><DummyTab number={3} /></Tab>
];

describe('Tabs', () => {
  it('Should set the first tab to active by default', () => {
    const el = shallow(<Tabs>{tabs}</Tabs>);
    expect(el.contains(<DummyTab number={1} />)).toBe(true);
    expect(el.find(DummyTab).length).toBe(1);
    expect(el.find('.active').text()).toContain('one');
  });

  it('Should allow you to specify a default via props', () => {
    const el = shallow(<Tabs activeTab='two'>{tabs}</Tabs>);
    expect(el.contains(<DummyTab number={2} />)).toBe(true);
    expect(el.find(DummyTab).length).toBe(1);
    expect(el.find('.active').text()).toContain('two');
  });

  it('Should switch tabs on click', () => {
    const el = shallow(<Tabs>{tabs}</Tabs>);
    expect(el.contains(<DummyTab number={1} />)).toBe(true);
    expect(el.find(DummyTab).length).toBe(1);
    expect(el.find('.active').text()).toContain('one');

    el.find('li').at(2).simulate('click');
    expect(el.contains(<DummyTab number={3} />)).toBe(true);
    expect(el.find('.active').text()).toContain('three');
  });
});
