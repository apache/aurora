import React from 'react';
import { shallow } from 'enzyme';

import RoleQuota, { QUOTA_TYPE_MAP } from '../RoleQuota';

function createResources(cpus, mem, disk) {
  return [{
    'numCpus': cpus
  }, {
    'ramMb': mem
  }, {
    'diskMb': disk
  }];
}

function initQuota() {
  const quota = {};
  Object.keys(QUOTA_TYPE_MAP).forEach((key) => {
    quota[key] = {resources: createResources(0, 0, 0)};
  });
  return quota;
}

describe('RoleQuota', () => {
  it('Should show all resources used', () => {
    Object.keys(QUOTA_TYPE_MAP).forEach((key) => {
      const quota = initQuota();
      quota[key] = {resources: createResources(10, 1024, 1024)};
      const el = shallow(<RoleQuota quota={quota} />);
      expect(el.contains(<tr key={key}>
        <td>{QUOTA_TYPE_MAP[key]}</td><td>{10}</td><td>{'1.00GiB'}</td><td>{'1.00GiB'}</td>
      </tr>)).toBe(true);
    });
  });
});
