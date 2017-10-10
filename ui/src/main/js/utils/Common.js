export function isNully(value) {
  return typeof value === 'undefined' || value === null;
}

export function invert(obj) {
  const inverted = {};
  Object.keys(obj).forEach((key) => {
    inverted[obj[key]] = key;
  });
  return inverted;
}

export function addClass(original, maybeClass) {
  if (isNully(maybeClass) || maybeClass.length === 0) {
    return original;
  }
  return `${original} ${maybeClass}`;
}

export function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}
