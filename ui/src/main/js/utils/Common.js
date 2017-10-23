export function isNully(value) {
  return typeof value === 'undefined' || value === null;
}

export function isNullyOrEmpty(value) {
  return isNully(value) || value.length === 0;
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

export function sort(arr, prop, reverse = false) {
  return arr.sort((a, b) => {
    if (prop(a) === prop(b)) {
      return 0;
    }
    if (prop(a) < prop(b)) {
      return reverse ? 1 : -1;
    }
    return reverse ? -1 : 1;
  });
}

export function pluralize(elements, singular, plural) {
  if (elements.length === 1) {
    return singular;
  }

  return (isNully(plural)) ? `${singular}s` : plural;
}

export function range(start, end) {
  return [...Array(1 + end - start).keys()].map((i) => start + i);
}
