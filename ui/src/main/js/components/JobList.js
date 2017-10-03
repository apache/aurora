import React from 'react';

import Icon from 'components/Icon';
import JobListItem from 'components/JobListItem';
import Pagination from 'components/Pagination';

import { isNully } from 'utils/Common';
import { TASK_COUNTS } from 'utils/Job';

export function JobListSortControl({ onClick }) {
  return (<ul className='job-task-stats job-list-sort-control'>
    <li>sort by:</li>
    {TASK_COUNTS.map((key) => {
      const label = key.replace('TaskCount', '');
      return (<li key={key} onClick={(e) => onClick(key)}>
        <span className={`img-circle ${label}-task`} /> {label}
      </li>);
    })}
  </ul>);
}

export default class JobList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {filter: props.filter, sortBy: props.sortBy};
  }

  setFilter(e) {
    this.setState({filter: e.target.value});
  }

  setSort(sortBy) {
    this.setState({sortBy});
  }

  _renderRow(job) {
    return <JobListItem job={job} key={`${job.job.key.environment}/${job.job.key.name}`} />;
  }

  render() {
    const that = this;
    const sortFn = this.state.sortBy ? (j) => j.stats[that.state.sortBy] : (j) => j.job.key.name;
    const filterFn = (j) => that.state.filter ? j.job.key.name.startsWith(that.state.filter) : true;
    return (<div className='job-list'>
      <div className='table-input-wrapper'>
        <Icon name='search' />
        <input
          autoFocus
          onChange={(e) => this.setFilter(e)}
          placeholder='Search jobs...'
          type='text' />
      </div>
      <JobListSortControl onClick={(key) => this.setSort(key)} />
      <table className='psuedo-table'>
        <Pagination
          data={this.props.jobs}
          filter={filterFn}
          isTable
          numberPerPage={25}
          renderer={this._renderRow}
          // Always sort task count sorts in descending fashion (for UX reasons)
          reverseSort={!isNully(this.state.sortBy)}
          sortBy={sortFn} />
      </table>
    </div>);
  }
}
