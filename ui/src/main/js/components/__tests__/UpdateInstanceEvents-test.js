import React from 'react';
import { shallow } from 'enzyme';

import Pagination from '../Pagination';
import StateMachine from '../StateMachine';
import UpdateInstanceEvents, { InstanceEvent } from '../UpdateInstanceEvents';

import { InstanceUpdateEventBuilder, UpdateDetailsBuilder } from 'test-utils/UpdateBuilders';

describe('UpdateInstanceEvents', () => {
  it('Should reverse sort each instance by the latest instance event', () => {
    const update = UpdateDetailsBuilder.instanceEvents([
      InstanceUpdateEventBuilder.instanceId(0).timestampMs(1).build(),
      InstanceUpdateEventBuilder.instanceId(1).timestampMs(0).build(),
      InstanceUpdateEventBuilder.instanceId(2).timestampMs(0).build(),
      InstanceUpdateEventBuilder.instanceId(2).timestampMs(2).build(),
      InstanceUpdateEventBuilder.instanceId(0).timestampMs(5).build(),
      InstanceUpdateEventBuilder.instanceId(3).timestampMs(0).build(),
      InstanceUpdateEventBuilder.instanceId(3).timestampMs(20).build(),
      InstanceUpdateEventBuilder.instanceId(4).timestampMs(3).build()]).build();

    const el = shallow(<UpdateInstanceEvents update={update} />);
    expect(el.find(Pagination).first().props().data).toEqual([3, 0, 4, 2, 1]);
  });
});

describe('InstanceEvent', () => {
  const jobKey = {job: {role: 'role', environment: 'env', name: 'name'}};

  it('Should support expand toggle', () => {
    const events = [
      InstanceUpdateEventBuilder.instanceId(0).timestampMs(1).build(),
      InstanceUpdateEventBuilder.instanceId(0).timestampMs(5).build()];

    const el = shallow(<InstanceEvent events={events} instanceId={0} jobKey={jobKey} />);
    expect(el.find(StateMachine).length).toBe(0);
    el.find('.update-instance-event').simulate('click');
    expect(el.find(StateMachine).length).toBe(1);
  });
});
