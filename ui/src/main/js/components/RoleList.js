import React from 'react';
import { Link } from 'react-router-dom';
import Reactable, { Table, Tr, Thead, Th, Td } from 'reactable';

import Icon from 'components/Icon';

export default class RoleList extends React.Component {
  setFilter(e) {
    this.setState({filter: e.target.value});
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
      <Table
        className='aurora-table'
        defaultSort={{column: 'role'}}
        filterBy={this.state.filter}
        filterPlaceholder='Search roles...'
        filterable={['role']}
        hideFilterInput
        itemsPerPage={25}
        noDataText={'No results found.'}
        pageButtonLimit={8}
        sortable={['role',
          {'column': 'jobs', sortFunction: Reactable.Sort.Numeric},
          {'column': 'crons', sortFunction: Reactable.Sort.Numeric}]}>
        <Thead>
          <Th column='role'>Role</Th>
          <Th className='number' column='jobs'>Jobs</Th>
          <Th className='number' column='crons'>Crons</Th>
        </Thead>
        {this.props.roles.map((r) => (<Tr key={r.role}>
          <Td column='role' value={r.role}><Link to={`/scheduler/${r.role}`}>{r.role}</Link></Td>
          <Td className='number' column='jobs'>{r.jobCount}</Td>
          <Td className='number' column='crons'>{r.cronJobCount}</Td>
        </Tr>))}
      </Table>
    </div>);
  }
}
