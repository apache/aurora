import {
  actionDispatcher,
  getClassForUpdateStatus,
  getClassForUpdateAction,
  instanceSummary,
  statusDispatcher,
  updateStats
} from '../Update';

import {
  InstanceTaskConfigBuilder,
  InstanceUpdateEventBuilder,
  UpdateBuilder,
  UpdateDetailsBuilder,
  UpdateInstructionsBuilder,
  builderWithStatus } from 'test-utils/UpdateBuilders';
import {
  ERROR_UPDATE_ACTION,
  ERROR_UPDATE_STATUS,
  WARNING_UPDATE_ACTION,
  WARNING_UPDATE_STATUS } from 'utils/Thrift';

function createDispatchSpies() {
  return {
    error: jest.fn(),
    inProgress: jest.fn(),
    success: jest.fn(),
    warning: jest.fn()
  };
}

function assertSpies(spies, expect) {
  return ['error', 'inProgress', 'success', 'warning'].every((key) => {
    const expected = expect[key] || 0;
    return spies[key].mock.calls.length === expected;
  });
}

describe('actionDispatcher', () => {
  it('Should dispatch success events', () => {
    const spies = createDispatchSpies();
    const dispatcher = actionDispatcher(spies);
    dispatcher(InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATED).build());
    expect(assertSpies(spies, {success: 1})).toBe(true);
  });

  it('Should dispatch in-progress events', () => {
    const spies = createDispatchSpies();
    const dispatcher = actionDispatcher(spies);
    dispatcher(InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATING).build());
    expect(assertSpies(spies, {inProgress: 1})).toBe(true);
  });

  it('Should dispatch warning events', () => {
    WARNING_UPDATE_ACTION.forEach((action) => {
      const spies = createDispatchSpies();
      const dispatcher = actionDispatcher(spies);
      dispatcher(InstanceUpdateEventBuilder.action(action).build());
      expect(assertSpies(spies, {warning: 1})).toBe(true);
    });
  });

  it('Should dispatch error events', () => {
    ERROR_UPDATE_ACTION.forEach((action) => {
      const spies = createDispatchSpies();
      const dispatcher = actionDispatcher(spies);
      dispatcher(InstanceUpdateEventBuilder.action(action).build());
      expect(assertSpies(spies, {error: 1})).toBe(true);
    });
  });
});

describe('getClassForUpdateAction', () => {
  it('Should return okay class', () => {
    expect(getClassForUpdateAction(JobUpdateAction.INSTANCE_UPDATED)).toBe('okay');
  });

  it('Should return in-progress class', () => {
    expect(getClassForUpdateAction(JobUpdateAction.INSTANCE_UPDATING)).toBe('in-progress');
  });

  it('Should dispatch warning events', () => {
    WARNING_UPDATE_ACTION.forEach((action) => {
      expect(getClassForUpdateAction(action)).toBe('attention');
    });
  });

  it('Should dispatch error events', () => {
    ERROR_UPDATE_ACTION.forEach((action) => {
      expect(getClassForUpdateAction(action)).toBe('error');
    });
  });
});

describe('getClassForUpdateStatus', () => {
  it('Should return okay for successful updates', () => {
    expect(getClassForUpdateStatus(JobUpdateStatus.ROLLED_FORWARD)).toBe('okay');
  });

  it('Should fire the in-progress callback for rolling forward updates', () => {
    expect(getClassForUpdateStatus(JobUpdateStatus.ROLLING_FORWARD)).toBe('in-progress');
  });

  it('Should fire the error callback for all failed updates', () => {
    ERROR_UPDATE_STATUS.forEach((status) => {
      expect(getClassForUpdateStatus(status)).toBe('error');
    });
  });

  it('Should fire the warning callback for all failed updates', () => {
    WARNING_UPDATE_STATUS.forEach((status) => {
      expect(getClassForUpdateStatus(status)).toBe('attention');
    });
  });
});

describe('instanceSummary', () => {
  const instanceUpdated = InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATED);

  it('Should return the correct data', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 10}]).build()])
      .desiredState(InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build())
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATE_FAILED).build(),
        instanceUpdated.instanceId(0).timestampMs(1).build(),
        instanceUpdated.instanceId(2).build(),
        instanceUpdated.instanceId(3).build(),
        instanceUpdated.instanceId(5).build(),
        instanceUpdated.instanceId(9).build()
      ])
      .build();
    const summary = instanceSummary(update);
    expect(summary).toEqual([
      {instanceId: '0', className: 'okay', title: 'INSTANCE_UPDATED'},
      {instanceId: '1', className: 'pending', title: 'Pending'},
      {instanceId: '2', className: 'okay', title: 'INSTANCE_UPDATED'},
      {instanceId: '3', className: 'okay', title: 'INSTANCE_UPDATED'},
      {instanceId: '4', className: 'pending', title: 'Pending'},
      {instanceId: '5', className: 'okay', title: 'INSTANCE_UPDATED'},
      {instanceId: '6', className: 'pending', title: 'Pending'},
      {instanceId: '7', className: 'pending', title: 'Pending'},
      {instanceId: '8', className: 'pending', title: 'Pending'},
      {instanceId: '9', className: 'okay', title: 'INSTANCE_UPDATED'},
      {instanceId: '10', className: 'pending', title: 'Pending'}
    ]);
  });

  it('Should handle removed instances', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 3}]).build()])
      .desiredState(InstanceTaskConfigBuilder.instances([{first: 0, last: 2}]).build())
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATE_FAILED).build(),
        InstanceUpdateEventBuilder.instanceId(2)
          .action(JobUpdateAction.INSTANCE_ROLLED_BACK)
          .build(),
        instanceUpdated.instanceId(3).build()
      ])
      .build();
    const summary = instanceSummary(update);
    expect(summary).toEqual([
      {instanceId: '0', className: 'error', title: 'INSTANCE_UPDATE_FAILED'},
      {instanceId: '1', className: 'pending', title: 'Pending'},
      {instanceId: '2', className: 'attention', title: 'INSTANCE_ROLLED_BACK'},
      {instanceId: '3', className: 'removed', title: 'Instance Removed'}
    ]);
  });
});

describe('statusDispatcher', () => {
  it('Should fire the success callback for successful updates', () => {
    const spies = createDispatchSpies();
    const dispatcher = statusDispatcher(spies);
    dispatcher(builderWithStatus(JobUpdateStatus.ROLLED_FORWARD).build());
    expect(assertSpies(spies, {success: 1})).toBe(true);
  });

  it('Should fire the in-progress callback for rolling forward updates', () => {
    const spies = createDispatchSpies();
    const dispatcher = statusDispatcher(spies);
    dispatcher(builderWithStatus(JobUpdateStatus.ROLLING_FORWARD).build());
    expect(assertSpies(spies, {inProgress: 1})).toBe(true);
  });

  it('Should fire the error callback for all failed updates', () => {
    ERROR_UPDATE_STATUS.forEach((status) => {
      const spies = createDispatchSpies();
      const dispatcher = statusDispatcher(spies);
      dispatcher(builderWithStatus(status).build());
      expect(assertSpies(spies, {error: 1})).toBe(true);
    });
  });

  it('Should fire the warning callback for all failed updates', () => {
    WARNING_UPDATE_STATUS.forEach((status) => {
      const spies = createDispatchSpies();
      const dispatcher = statusDispatcher(spies);
      dispatcher(builderWithStatus(status).build());
      expect(assertSpies(spies, {warning: 1})).toBe(true);
    });
  });
});

describe('updateStats', () => {
  const instanceUpdated = InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATED);

  it('Should return the correct stats for a job with some instances to be updated', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build()])
      .desiredState(InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build())
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        InstanceUpdateEventBuilder.action(JobUpdateAction.INSTANCE_UPDATE_FAILED).build(),
        instanceUpdated.instanceId(0).build(),
        instanceUpdated.instanceId(2).build(),
        instanceUpdated.instanceId(3).build(),
        instanceUpdated.instanceId(5).build(),
        instanceUpdated.instanceId(9).build()
      ])
      .build();
    const stats = updateStats(update);
    expect(stats).toEqual({totalInstancesToBeUpdated: 10, instancesUpdated: 5, progress: 50});
  });

  it('Should respect added instances', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 4}]).build()])
      .desiredState(InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build())
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        instanceUpdated.instanceId(7).build(),
        instanceUpdated.instanceId(8).build()
      ])
      .build();
    const stats = updateStats(update);
    expect(stats).toEqual({totalInstancesToBeUpdated: 10, instancesUpdated: 2, progress: 20});
  });

  it('Should respect deleted instances', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build()])
      .desiredState(InstanceTaskConfigBuilder.instances([{first: 0, last: 4}]).build())
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        instanceUpdated.instanceId(7).build(),
        instanceUpdated.instanceId(8).build()
      ])
      .build();
    const stats = updateStats(update);
    expect(stats).toEqual({totalInstancesToBeUpdated: 10, instancesUpdated: 2, progress: 20});
  });

  it('Should respect when there are exclusively deleted instances', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 1}]).build()])
      .desiredState(null)
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        instanceUpdated.instanceId(0).build(),
        instanceUpdated.instanceId(1).build()
      ])
      .build();
    const stats = updateStats(update);
    expect(stats).toEqual({totalInstancesToBeUpdated: 2, instancesUpdated: 2, progress: 100});
  });

  it('Any instances updated should show up in stats, even if rolled back', () => {
    const instructions = UpdateInstructionsBuilder
      .initialState([InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build()])
      .desiredState(InstanceTaskConfigBuilder.instances([{first: 0, last: 9}]).build())
      .build();

    const update = UpdateDetailsBuilder
      .update(UpdateBuilder.instructions(instructions).build())
      .instanceEvents([
        instanceUpdated.instanceId(0).build(),
        instanceUpdated.instanceId(2).build(),
        InstanceUpdateEventBuilder.instanceId(2)
          .action(JobUpdateAction.INSTANCE_UPDATE_FAILED)
          .timestampMs(2)
          .build()
      ])
      .build();
    const stats = updateStats(update);
    expect(stats).toEqual({totalInstancesToBeUpdated: 10, instancesUpdated: 2, progress: 20});
  });
});
