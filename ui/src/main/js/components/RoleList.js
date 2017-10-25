import React from 'react';
import { Link } from 'react-router-dom';

import Icon from 'components/Icon';
import Pagination from 'components/Pagination';

export default class RoleList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      filter: props.filter,
      reverseSort: props.reverseSort || false,
      sortBy: props.sortBy || 'role'
    };
  }

  setFilter(e) {
    this.setState({filter: e.target.value, sortBy: 'role'});
  }

  setSort(sortBy) {
    // If they change sort key, it's always ascending the first time.
    const reverseSort = (sortBy === this.state.sortBy) ? !this.state.reverseSort : false;
    this.setState({reverseSort, sortBy});
  }

  _renderRow(r) {
    return (<tr key={r.role}>
      <td><Link to={`/scheduler/${r.role}`}>{r.role}</Link></td>
      <td>{r.jobCount}</td>
      <td>{r.cronJobCount}</td>
    </tr>);
  }

  render() {
    return (<div className='role-list'>
      <div className='table-input-wrapper'>
        <Icon name='search' />
        <input
          autoFocus
          onChange={(e) => this.setFilter(e)}
          placeholder='Search for roles'
          type='text' />
      </div>
      <table className='aurora-table'>
        <thead>
          <tr>
            <th onClick={(e) => this.setSort('role')}>Role</th>
            <th onClick={(e) => this.setSort('jobCount')}>Jobs</th>
            <th onClick={(e) => this.setSort('cronJobCount')}>Crons</th>
          </tr>
        </thead>
        <Pagination
          data={this.props.roles}
          filter={(r) => (this.state.filter) ? r.role.startsWith(this.state.filter) : true}
          hideIfSinglePage
          isTable
          numberPerPage={25}
          renderer={this._renderRow}
          reverseSort={this.state.reverseSort}
          sortBy={this.state.sortBy} />
      </table>
    </div>);
  }
}
