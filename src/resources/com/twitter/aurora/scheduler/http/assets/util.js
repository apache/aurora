/**
 * Given an epoch milliseconds timestamp, generate a string representation that includes
 * GMT and local time.
 */
function printDate(timestamp) {
  function pad(number) {
    number = '' + number;
    if (number.length < 2) {
      return '0' + number;
    }
    return number;
  }
  var d = new Date(timestamp);
  return pad(d.getUTCMonth() + 1) + "/"
      + pad(d.getUTCDate()) + " "
      + pad(d.getUTCHours()) + ":"
      + pad(d.getUTCMinutes()) + ":"
      + pad(d.getUTCSeconds())
      + ' UTC ('
      + pad(d.getMonth() + 1) + "/"
      + pad(d.getDate()) + " "
      + pad(d.getHours()) + ":"
      + pad(d.getMinutes()) + ":"
      + pad(d.getSeconds())
      + ' local)';
}

/**
 * Given the current time and an epoch milliseconds timestamp, generate a human-readable
 * representation of how long has elapsed from timestamp to now.
 */
function printElapsed(newerTimestamp, olderTimestamp) {
  var elapsed = newerTimestamp - olderTimestamp;
  if (elapsed <= 0) {
    return "epsilon";
  }
  var SEC = 1000;
  var MIN = 60 * SEC;
  var HOUR = 60 * MIN;
  var DAY = 24 * HOUR;
  if (elapsed < 2 * SEC) {
    return Math.floor(elapsed) + " msec";
  }
  if (elapsed < 2 * MIN) {
    return Math.floor(elapsed / SEC) + " secs";
  }
  if (elapsed < 2 * HOUR) {
    return Math.floor(elapsed / MIN) + " mins";
  }
  if (elapsed < 2 * DAY) {
    return Math.floor(elapsed / HOUR) + " hours";
  }
  return Math.floor(elapsed / DAY) + " days";
}
