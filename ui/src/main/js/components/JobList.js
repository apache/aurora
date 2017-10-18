import React from 'react';

import Icon from 'components/Icon';
import JobListItem from 'components/JobListItem';
import Pagination from 'components/Pagination';

import { isNully } from 'utils/Common';
import { TASK_COUNTS } from 'utils/Job';

// VisibleForTesting
export function searchJob(job, query) {
  const taskConfig = job.job.taskConfig;
  const jobType = taskConfig.isService ? 'service' : job.job.cronSchedule ? 'cron' : 'adhoc';
  return (job.job.key.name.startsWith(query) ||
    taskConfig.tier.startsWith(query) ||
    job.job.key.environment.startsWith(query) ||
    jobType.startsWith(query));
}

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

  render() {
    const that = this;
    const sortFn = this.state.sortBy ? (j) => j.stats[that.state.sortBy] : (j) => j.job.key.name;
    const filterFn = (j) => that.state.filter ? searchJob(j, that.state.filter) : true;
    return (<div className='job-list'>
      <div className='table-input-wrapper'>
        <Icon name='search' />
        <input
          autoFocus
          onChange={(e) => this.setFilter(e)}
          placeholder='Search jobs by name, environment, type or tier'
          type='text' />
      </div>
      <JobListSortControl onClick={(key) => this.setSort(key)} />
      <table className='psuedo-table'>
        <Pagination
          data={this.props.jobs}
          filter={filterFn}
          hideIfSinglePage
          isTable
          numberPerPage={25}
          renderer={(job) => <JobListItem
            env={that.props.env}
            job={job}
            key={`${job.job.key.environment}/${job.job.key.name}`} />}
          // Always sort task count sorts in descending fashion (for UX reasons)
          reverseSort={!isNully(this.state.sortBy)}
          sortBy={sortFn} />
      </table>
    </div>);
  }
}
