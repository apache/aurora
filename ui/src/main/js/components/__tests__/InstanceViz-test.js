import React from 'react';
import { shallow } from 'enzyme';

import InstanceViz from '../InstanceViz';

import { range } from 'utils/Common';

function generateInstances(n) {
  return range(0, n - 1).map((i) => {
    return {
      className: 'okay',
      instanceId: i,
      title: `test-${i}`
    };
  });
}

const jobKey = {job: {role: 'test', environment: 'test', name: 'test'}};

describe('InstanceViz', () => {
  it('Should apply the small class to large numbers of instances', () => {
    const el = shallow(<InstanceViz instances={generateInstances(1001)} jobKey={jobKey} />);
    expect(el.find('ul.small').length).toBe(1);
    expect(el.find('ul.medium').length).toBe(0);
    expect(el.find('ul.big').length).toBe(0);
  });

  it('Should apply the medium class to medium numbers of instances', () => {
    const el = shallow(<InstanceViz instances={generateInstances(101)} jobKey={jobKey} />);
    expect(el.find('ul.small').length).toBe(0);
    expect(el.find('ul.medium').length).toBe(1);
    expect(el.find('ul.big').length).toBe(0);
  });

  it('Should apply the big class to small numbers of instances', () => {
    const el = shallow(<InstanceViz instances={generateInstances(100)} jobKey={jobKey} />);
    expect(el.find('ul.small').length).toBe(0);
    expect(el.find('ul.medium').length).toBe(0);
    expect(el.find('ul.big').length).toBe(1);
  });
});
