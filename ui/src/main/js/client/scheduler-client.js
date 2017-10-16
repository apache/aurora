import { isNully } from 'utils/Common';

function getBaseUrl() {
  // Power feature: load a different Scheduler URL from local storage to test against live APIs.
  //
  // In your browser console:
  //    window.localStorage.apiUrl = 'http://myauroracluster.com';
  //
  // To remove the item:
  //    window.localStorage.removeItem('apiUrl')
  if (window && window.localStorage) {
    return isNully(window.localStorage.apiUrl) ? '' : window.localStorage.apiUrl;
  }
  return '';
}

function makeClient() {
  const transport = new Thrift.Transport(`${getBaseUrl()}/api`);
  const protocol = new Thrift.Protocol(transport);
  return new ReadOnlySchedulerClient(protocol);
}

export default makeClient();
