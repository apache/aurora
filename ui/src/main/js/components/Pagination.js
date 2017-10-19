import React from 'react';

export function PageNavigation({currentPage, maxPages, numPages, onClick}) {
  // Pad the current page on both sides by one half of maxPages
  const lastPage = Math.min(currentPage + Math.round(maxPages / 2), numPages);
  const firstPage = Math.max(currentPage - Math.round(maxPages / 2), 1);

  const pages = [];
  for (let i = firstPage; i <= lastPage; i++) {
    if (i === currentPage) {
      pages.push(<li className='active' key={i}><span>{i}</span></li>);
    } else {
      pages.push(<li key={i}><a onClick={(e) => onClick(i)}>{i}</a></li>);
    }
  }

  const prevPage = (currentPage > 1)
    ? <li key='prev'><a onClick={(e) => onClick(currentPage - 1)}>&laquo;</a></li>
    : '';
  const nextPage = (currentPage < numPages)
    ? <li key='next'><a onClick={(e) => onClick(currentPage + 1)}>&raquo;</a></li>
    : '';

  return (<div className='pagination-wrapper'>
    <ul className='pagination'>
      {prevPage}
      {pages}
      {nextPage}
    </ul>
  </div>);
}

export default class Pagination extends React.Component {
  constructor(props) {
    super(props);
    this.state = {page: props.page || 1};
  }

  componentWillReceiveProps(nextProps) {
    // Make sure to reset page when props change (caused by data change or sort change or filter)
    if (this.state.page > 1) {
      this.setState({page: 1});
    }
  }

  changePage(page) {
    this.setState({page});
  }

  filter(data) {
    if (this.props.filter) {
      return data.filter(this.props.filter);
    }
    return data;
  }

  sort(data) {
    const { reverseSort, sortBy } = this.props;
    if (!sortBy) {
      return data;
    }

    const gte = reverseSort ? -1 : 1;
    const lte = reverseSort ? 1 : -1;
    if (typeof sortBy === 'function') {
      return data.sort((a, b) => {
        return (sortBy(a) > sortBy(b)) ? gte : lte;
      });
    }
    return data.sort((a, b) => {
      return (a[sortBy] > b[sortBy]) ? gte : lte;
    });
  }

  render() {
    const that = this;
    const { data, isTable, maxPages, numberPerPage, renderer, hideIfSinglePage } = this.props;
    const { page } = this.state;

    // Apply the filter before we try to paginate.
    const filtered = this.filter(data);

    // Figure out the slice of the array that represents the current page.
    const firstIdx = (page - 1) * numberPerPage;
    const lastIdx = firstIdx + numberPerPage;
    const currentPageItems = this.sort(filtered).slice(firstIdx, lastIdx);

    // A cleaner interface would be to just pass in a React Component as the renderer:
    //    <Pagination ... renderer={MyItem} />
    // And then pass in the data as props to their component. Which we can support with:
    //    currentPage.map((item) => React.createElement(renderer, item))
    // but first attempts at this broke shallow rendering in enzyme.
    const elements = currentPageItems.map(renderer);

    const numPages = Math.ceil(filtered.length / numberPerPage);

    // The clickable page list.
    const pagination = (numPages === 1 && hideIfSinglePage) ? null : <PageNavigation
      currentPage={page}
      maxPages={maxPages || 8}
      numPages={Math.ceil(filtered.length / numberPerPage)}
      onClick={(page) => that.changePage(page)} />;

    // We need the caller to be able to signify they are paging through a table element so
    // we know to wrap the pagination links in a tr.
    if (isTable) {
      return (<tbody>
        {elements}
        {pagination
          ? <tr className='pagination-row'><td colSpan='100%'>{pagination}</td></tr>
          : null}
      </tbody>);
    }
    return <div>{elements}{pagination}</div>;
  }
}
