import React from 'react';
import { shallow } from 'enzyme';

import Pagination, { PageNavigation } from '../Pagination';

const data = [
  {row: 1, name: 'one'},
  {row: 2, name: 'two'},
  {row: 3, name: 'three'},
  {row: 4, name: 'four'},
  {row: 5, name: 'five'},
  {row: 6, name: 'six'},
  {row: 7, name: 'seven'},
  {row: 8, name: 'eight'},
  {row: 9, name: 'nine'},
  {row: 10, name: 'ten'}
];

function Row({ data }) {
  return <span>{data.row} - {data.name}</span>;
}

function render(data) {
  return <Row data={data} key={data.row} />;
}

describe('Pagination', () => {
  it('Should render the first page by default', () => {
    const el = shallow(<Pagination data={data} numberPerPage={3} renderer={render} />);
    expect(el.find(Row).length).toBe(3);
    expect(el.is('div')).toBe(true);
    expect(el.containsAllMatchingElements([
      <Row data={data[0]} key={1} />,
      <Row data={data[1]} key={2} />,
      <Row data={data[2]} key={3} />,
      <PageNavigation currentPage={1} numPages={4} />])).toBe(true);
  });

  it('Should render other pages when set as props', () => {
    const el = shallow(<Pagination data={data} numberPerPage={3} page={2} renderer={render} />);
    expect(el.find(Row).length).toBe(3);
    expect(el.containsAllMatchingElements([
      <Row data={data[3]} key={4} />,
      <Row data={data[4]} key={5} />,
      <Row data={data[5]} key={6} />,
      <PageNavigation currentPage={2} numPages={4} />])).toBe(true);
  });

  it('Should handle a single page', () => {
    const el = shallow(<Pagination data={data} numberPerPage={25} renderer={render} />);
    expect(el.find(Row).length).toBe(10);
    expect(
      el.containsAllMatchingElements([<PageNavigation currentPage={1} numPages={1} />])).toBe(true);
  });

  it('Should not show PageNavigation when hide single page is set', () => {
    const el = shallow(
      <Pagination data={data} hideIfSinglePage numberPerPage={25} renderer={render} />);
    expect(el.find(Row).length).toBe(10);
    expect(el.find(PageNavigation).length).toBe(0);
  });

  it('Should show PageNavigation when hide single page is set, but theres multiple pages', () => {
    const el = shallow(
      <Pagination data={data} hideIfSinglePage numberPerPage={2} renderer={render} />);
    expect(el.find(Row).length).toBe(2);
    expect(el.find(PageNavigation).length).toBe(1);
  });

  it('Should sort correctly', () => {
    const el = shallow(
      <Pagination data={data} numberPerPage={3} renderer={render} sortBy='name' />);
    expect(el.find(Row).length).toBe(3);
    expect(el.containsAllMatchingElements([
      <Row key={8} />,
      <Row key={4} />,
      <Row key={5} />,
      <PageNavigation currentPage={1} numPages={4} />])).toBe(true);
  });

  it('Reverse sort correctly', () => {
    const el = shallow(
      <Pagination data={data} numberPerPage={3} renderer={render} reverseSort sortBy='name' />);
    expect(el.find(Row).length).toBe(3);
    expect(el.containsAllMatchingElements([
      <Row key={2} />,
      <Row key={3} />,
      <Row key={10} />,
      <PageNavigation currentPage={1} numPages={4} />])).toBe(true);
  });

  it('Should respect natural order when sortBy is omitted', () => {
    const el = shallow(
      <Pagination data={data} numberPerPage={3} renderer={render} />);
    expect(el.find(Row).length).toBe(3);
    expect(el.containsAllMatchingElements([
      <Row key={1} />,
      <Row key={2} />,
      <Row key={3} />,
      <PageNavigation currentPage={1} numPages={4} />])).toBe(true);
  });

  it('Should filter correctly', () => {
    const el = shallow(<Pagination
      data={data}
      filter={(d) => d.name === 'one'}
      numberPerPage={3}
      renderer={render} />);
    expect(el.find(Row).length).toBe(1);
    expect(el.containsAllMatchingElements([
      <Row key={1} />,
      <PageNavigation currentPage={1} numPages={1} />])).toBe(true);
  });

  it('Should change page when state is updated', () => {
    const el = shallow(
      <Pagination data={data} numberPerPage={3} page={1} renderer={render} />);
    expect(el.containsAllMatchingElements([
      <Row key={1} />,
      <Row key={2} />,
      <Row key={3} />,
      <PageNavigation currentPage={1} numPages={4} />])).toBe(true);
    el.setState({page: 2});
    expect(el.containsAllMatchingElements([
      <Row key={4} />,
      <Row key={5} />,
      <Row key={6} />,
      <PageNavigation currentPage={2} numPages={4} />])).toBe(true);
  });

  it('Should reset pagination when *any* new props are set', () => {
    const el = shallow(
      <Pagination data={data} numberPerPage={3} page={2} renderer={render} />);
    expect(el.containsAllMatchingElements([
      <Row key={4} />,
      <Row key={5} />,
      <Row key={6} />,
      <PageNavigation currentPage={2} numPages={4} />])).toBe(true);
    el.setProps({
      data: data,
      numberPerPage: 3,
      page: 2,
      renderer: render
    });
    expect(el.containsAllMatchingElements([
      <Row key={1} />,
      <Row key={2} />,
      <Row key={3} />,
      <PageNavigation currentPage={1} numPages={4} />])).toBe(true);
  });

  it('Should render into a tbody when isTable is set', () => {
    const el = shallow(<Pagination data={data} isTable numberPerPage={3} renderer={render} />);
    expect(el.is('tbody')).toBe(true);
  });
});

describe('PageNavigation', () => {
  it('Should handle a single page navigation', () => {
    const el = shallow(<PageNavigation currentPage={1} maxPages={5} numPages={1} />);
    expect(el.contains(<li className='active' key={1}><span>{1}</span></li>)).toBe(true);
  });

  it('Should handle a multi page navigation starting from 1st page', () => {
    const el = shallow(<PageNavigation currentPage={1} maxPages={5} numPages={10} />);
    expect(el.find('li').length).toBe(5);
    expect(el.containsAllMatchingElements([
      <li className='active'><span>{1}</span></li>,
      <li><a>{2}</a></li>,
      <li><a>{3}</a></li>,
      <li><a>{4}</a></li>,
      <li><a>&raquo;</a></li>
    ])).toBe(true);
  });

  it('Should handle a multi page navigation starting from last page', () => {
    const el = shallow(<PageNavigation currentPage={10} maxPages={5} numPages={10} />);
    expect(el.find('li').length).toBe(5);
    expect(el.containsAllMatchingElements([
      <li className='active'><span>{10}</span></li>,
      <li><a>{9}</a></li>,
      <li><a>{8}</a></li>,
      <li><a>{7}</a></li>,
      <li><a>&laquo;</a></li>
    ])).toBe(true);
  });

  it('Should handle a multi page navigation starting from a middle page', () => {
    const el = shallow(<PageNavigation currentPage={5} maxPages={5} numPages={10} />);
    expect(el.find('li').length).toBe(9);
    expect(el.containsAllMatchingElements([
      <li className='active'><span>{5}</span></li>,
      <li><a>{4}</a></li>,
      <li><a>{3}</a></li>,
      <li><a>{6}</a></li>,
      <li><a>{7}</a></li>,
      <li><a>{8}</a></li>,
      <li><a>&laquo;</a></li>,
      <li><a>&raquo;</a></li>
    ])).toBe(true);
  });

  it('Should pass the correct page when an item is clicked', () => {
    const tracking = {};
    const click = (page) => {
      tracking.clicked = page;
    };
    const el = shallow(
      <PageNavigation currentPage={1} maxPages={5} numPages={3} onClick={click} />);
    // Find the next page link and click it
    el.find('a').last().simulate('click');
    expect(tracking.clicked).toBe(2);
    // Click individual pages
    el.find('a').at(1).simulate('click');
    expect(tracking.clicked).toBe(3);
    // Click individual pages
    el.find('a').at(0).simulate('click');
    expect(tracking.clicked).toBe(2);
  });
});
