import React from 'react';
import { shallow } from 'enzyme';

import Tabs from '../Tabs';

const DummyTab = ({ number }) => <span>Hello, {number}</span>;

const tabs = [
  {id: 'one', name: 'one', content: <DummyTab number={1} />},
  {id: 'two', name: 'two', content: <DummyTab number={2} />},
  {id: 'three', name: 'three', content: <DummyTab number={3} />}
];

describe('Tabs', () => {
  it('Should set the first tab to active by default', () => {
    const el = shallow(<Tabs tabs={tabs} />);
    expect(el.contains(<DummyTab number={1} />)).toBe(true);
    expect(el.find(DummyTab).length).toBe(1);
    expect(el.find('.active').key()).toBe('one');
  });

  it('Should allow you to specify a default via props', () => {
    const el = shallow(<Tabs activeTab='two' tabs={tabs} />);
    expect(el.contains(<DummyTab number={2} />)).toBe(true);
    expect(el.find(DummyTab).length).toBe(1);
    expect(el.find('.active').key()).toBe('two');
  });

  it('Should switch tabs on click', () => {
    const el = shallow(<Tabs tabs={tabs} />);
    expect(el.contains(<DummyTab number={1} />)).toBe(true);
    expect(el.find(DummyTab).length).toBe(1);
    expect(el.find('.active').key()).toBe('one');

    el.find('li').at(2).simulate('click');
    expect(el.contains(<DummyTab number={3} />)).toBe(true);
    expect(el.find('.active').key()).toBe('three');
  });
});
