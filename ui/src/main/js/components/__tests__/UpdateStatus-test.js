import React from 'react';
import { shallow } from 'enzyme';

import UpdateStatus from '../UpdateStatus';
import UpdateTime from '../UpdateTime';

import { builderWithStatus } from 'test-utils/UpdateBuilders';

describe('UpdateStatus', () => {
  it('Should show UpdateTime when update terminal', () => {
    const update = builderWithStatus(JobUpdateStatus.ROLLED_FORWARD).build();

    const el = shallow(<UpdateStatus update={update} />);
    expect(el.contains(<UpdateTime update={update} />)).toBe(true);
  });

  it('Should NOT show UpdateTime when update in-progress', () => {
    const update = builderWithStatus(JobUpdateStatus.ROLLING_FORWARD).build();

    const el = shallow(<UpdateStatus update={update} />);
    expect(el.find(UpdateTime).length).toBe(0);
  });
});
