import React from 'react';
import { shallow } from 'enzyme';

import JobList from '../JobList';
import Pagination from '../Pagination';

const jobs = []; // only need referential equality for tests

describe('JobList', () => {
  it('Should delegate all the heavy lifting to Pagination', () => {
    const el = shallow(<JobList jobs={jobs} />);
    expect(el.find(Pagination).length).toBe(1);
    expect(el.containsAllMatchingElements([
      <Pagination data={jobs} reverseSort={false} />
    ])).toBe(true);
  });

  it('Should reverse sort whenever a task sort is supplied', () => {
    const el = shallow(<JobList jobs={jobs} sortBy='test' />);
    expect(el.find(Pagination).length).toBe(1);
    expect(el.containsAllMatchingElements([
      <Pagination data={jobs} reverseSort />
    ])).toBe(true);
  });
});
