import { sort } from 'utils/Common';
import Thrift, { UPDATE_ACTION } from 'utils/Thrift';

export function isSuccessfulUpdate(update) {
  return update.update.summary.state.status === JobUpdateStatus.ROLLED_FORWARD;
}

export function isInProgressUpdate(update) {
  return update.update.summary.state.status === JobUpdateStatus.ROLLING_FORWARD;
}

function processInstanceIdsFromRanges(ranges, fn) {
  ranges.forEach((r) => {
    for (let i = r.first; i <= r.last; i++) {
      fn(i);
    }
  });
}

function getAllInstanceIds(update) {
  const allIds = {};
  const newIds = {};
  const oldIds = {};

  processInstanceIdsFromRanges(update.instructions.desiredState.instances, (id) => {
    newIds[id] = true;
    allIds[id] = true;
  });

  update.instructions.initialState.forEach((task) => {
    processInstanceIdsFromRanges(task.instances, (id) => {
      oldIds[id] = true;
      allIds[id] = true;
    });
  });
  return { allIds, newIds, oldIds };
}

function getLatestInstanceEvents(instanceEvents, predicate = (e) => true) {
  const events = sort(instanceEvents, (e) => e.timestampMs, true);
  const instanceMap = {};
  events.forEach((e) => {
    if (!instanceMap.hasOwnProperty(e.instanceId) && predicate(e)) {
      instanceMap[e.instanceId] = e;
    }
  });
  return instanceMap;
}

export function instanceSummary(details) {
  const instances = getAllInstanceIds(details.update);
  const latestInstanceEvents = getLatestInstanceEvents(details.instanceEvents);
  const allIds = Object.keys(instances.allIds);

  return allIds.map((i) => {
    // If there is an event, use the event to generate the instance status.
    if (latestInstanceEvents.hasOwnProperty(i)) {
      const latestEvent = latestInstanceEvents[i];
      // If instance has been updated and is in initial state, but not in desired state,
      // then it's a removed instance.
      if (latestEvent.action === JobUpdateAction.INSTANCE_UPDATED &&
          instances.oldIds.hasOwnProperty(i) &&
          !instances.newIds.hasOwnProperty(i)) {
        return {
          instanceId: i,
          className: 'removed',
          title: 'Instance Removed'
        };
      }

      // Normal case - the latest action is the current status
      return {
        instanceId: i,
        className: getClassForUpdateAction(latestEvent.action),
        title: UPDATE_ACTION[latestEvent.action]
      };
    } else {
      // No event, so it's a pending instance.
      return {
        instanceId: i,
        className: 'pending',
        title: 'Pending'
      };
    }
  });
}

function progressFromEvents(instanceEvents) {
  const success = getLatestInstanceEvents(instanceEvents,
    (e) => e.action === JobUpdateAction.INSTANCE_UPDATED);
  return Object.keys(success).length;
}

export function updateStats(details) {
  const allInstances = Object.keys(getAllInstanceIds(details.update).allIds);
  const totalInstancesToBeUpdated = allInstances.length;
  const instancesUpdated = progressFromEvents(details.instanceEvents);
  const progress = Math.round((instancesUpdated / totalInstancesToBeUpdated) * 100);
  return {
    totalInstancesToBeUpdated,
    instancesUpdated,
    progress
  };
}

export function getInProgressStates() {
  return ACTIVE_JOB_UPDATE_STATES;
}

export function getTerminalStates() {
  const active = new Set(ACTIVE_JOB_UPDATE_STATES);
  return Object.values(JobUpdateStatus).filter((k) => !active.has(k));
}

export function getClassForUpdateStatus(status) {
  if (Thrift.OKAY_UPDATE_STATUS.includes(status)) {
    return 'okay';
  } else if (Thrift.WARNING_UPDATE_STATUS.includes(status)) {
    return 'attention';
  } else if (Thrift.ERROR_UPDATE_STATUS.includes(status)) {
    return 'error';
  }
  return 'in-progress';
}

export function getClassForUpdateAction(action) {
  if (Thrift.OKAY_UPDATE_ACTION.includes(action)) {
    return 'okay';
  } else if (Thrift.WARNING_UPDATE_ACTION.includes(action)) {
    return 'attention';
  } else if (Thrift.ERROR_UPDATE_ACTION.includes(action)) {
    return 'error';
  }
  return 'in-progress';
}

export function statusDispatcher(dispatch) {
  return (update) => {
    const status = update.update.summary.state.status;
    if (Thrift.OKAY_UPDATE_STATUS.includes(status)) {
      return dispatch.success(update);
    } else if (Thrift.WARNING_UPDATE_STATUS.includes(status)) {
      return dispatch.warning(update);
    } else if (Thrift.ERROR_UPDATE_STATUS.includes(status)) {
      return dispatch.error(update);
    }
    return dispatch.inProgress(update);
  };
}

export function actionDispatcher(dispatch) {
  return (event) => {
    const action = event.action;
    if (Thrift.OKAY_UPDATE_ACTION.includes(action)) {
      return dispatch.success(event);
    } else if (Thrift.WARNING_UPDATE_ACTION.includes(action)) {
      return dispatch.warning(event);
    } else if (Thrift.ERROR_UPDATE_ACTION.includes(action)) {
      return dispatch.error(event);
    }
    return dispatch.inProgress(event);
  };
}
