import React from 'react';
import { shallow } from 'enzyme';

import JobList, { searchJob } from '../JobList';
import Pagination from '../Pagination';

import { JobSummaryBuilder, JobConfigurationBuilder } from 'test-utils/JobBuilders';
import { TaskConfigBuilder } from 'test-utils/TaskBuilders';

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

describe('searchJob', () => {
  it('Should match jobs by job key properties', () => {
    const job = JobSummaryBuilder.job(
      JobConfigurationBuilder.key({role: '', environment: 'zzz-env', name: 'yyy-name'}).build()
    ).build();

    expect(searchJob(job, '___notfound__')).toBe(false);
    expect(searchJob(job, 'zzz')).toBe(true);
    expect(searchJob(job, 'yyy')).toBe(true);
    expect(searchJob(job, 'y-n')).toBe(true);
  });

  it('Should match jobs by tier', () => {
    const job = JobSummaryBuilder.job(
      JobConfigurationBuilder
        .key({role: '_', environment: '_', name: '_'})
        .taskConfig(TaskConfigBuilder.tier('zzz-tier').build())
        .build()
    ).build();

    expect(searchJob(job, 'zzz')).toBe(true);
  });

  it('Should allow you to search for services', () => {
    const job = JobSummaryBuilder.job(
      JobConfigurationBuilder
        .key({role: '', environment: 'zzz-env', name: 'yyy-name'})
        .taskConfig(TaskConfigBuilder.isService(true).build())
        .build()
    ).build();

    expect(searchJob(job, 'service')).toBe(true);
    expect(searchJob(job, 'adhoc')).toBe(false);
    expect(searchJob(job, 'cron')).toBe(false);
  });

  it('Should allow you to search for crons', () => {
    const job = JobSummaryBuilder.job(
      JobConfigurationBuilder
        .key({role: '', environment: 'zzz-env', name: 'yyy-name'})
        .cronSchedule('something-present')
        .taskConfig(TaskConfigBuilder.isService(false).build())
        .build()
    ).build();

    expect(searchJob(job, 'service')).toBe(false);
    expect(searchJob(job, 'adhoc')).toBe(false);
    expect(searchJob(job, 'cron')).toBe(true);
  });

  it('Should allow you to search for adhoc jobs', () => {
    const job = JobSummaryBuilder.job(
      JobConfigurationBuilder
        .key({role: '', environment: 'zzz-env', name: 'yyy-name'})
        .taskConfig(TaskConfigBuilder.isService(false).build())
        .build()
    ).build();

    expect(searchJob(job, 'service')).toBe(false);
    expect(searchJob(job, 'adhoc')).toBe(true);
    expect(searchJob(job, 'cron')).toBe(false);
  });
});
