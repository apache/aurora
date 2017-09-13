import { options, render } from 'preact';
import deepEqual from 'deep-equal';

function propsForElement(el) {
  return el.__preactattr_ || {};
}

function extractName(vnode) {
  return (typeof vnode.nodeName === 'string')
    ? vnode.nodeName
    : (vnode.nodeName.prototype.displayName || vnode.nodeName.name);
}

function textChildrenMatch(domNode, vnode) {
  const textChildren = vnode.children.filter((c) => typeof c === 'string').map((s) => s.trim());
  if (textChildren.length === 0) {
    return true;
  }
  return textChildren.join(' ') === domNode.innerText.replace(/ +(?= )/g, '');
}

function findInSiblings(domNode, vnode) {
  let cursor = domNode.nextElementSibling;
  while (cursor !== null) {
    if (matches(cursor, vnode)) {
      return cursor;
    }
    cursor = cursor.nextElementSibling;
  }
  return null;
}

function hasSiblings(domNode, vnodes) {
  let cursor = domNode;
  const found = [];
  vnodes.forEach((node) => {
    if (cursor !== null) {
      cursor = findInSiblings(cursor, node);
      if (cursor) {
        found.push(cursor);
      }
    }
  });
  return found.length === vnodes.length;
}

function vnodeChildrenPresent(domNode, vnode) {
  const vnodeChildren = vnode.children.filter((c) => typeof c !== 'string');
  if (vnodeChildren.length === 0) {
    return true;
  }

  // for children we want to maintain two key properties when matching:
  //  * order of nodes must match
  //  * number of nodes should match
  // to do this we try and find all matches for vnodeChildren[0] and then
  // use the sibling API to verify the rest of the children are present at the same
  // level in the DOM tree
  const [head, ...tail] = vnodeChildren;

  const matches = allMatches(domNode, head);

  for (let i = 0; i < matches.length; i++) {
    if (hasSiblings(matches[i], tail)) {
      return true;
    }
  }

  return false;
}

function childrenMatch(domNode, vnode) {
  if (vnode.attributes.children.length === 0) {
    return true;
  }
  return textChildrenMatch(domNode, vnode) && vnodeChildrenPresent(domNode, vnode);
}

function propertiesMatch(domNode, vnode) {
  const domProperties = propsForElement(domNode);
  const vnodeProperties = vnode.attributes;
  const defaultProperties = vnode.nodeName.defaultProps || {};

  return Object.keys(vnodeProperties).reduce((matches, key) => {
    if (key === 'children') {
      return matches && childrenMatch(domNode, vnode);
    }
    if (defaultProperties.hasOwnProperty(key) && vnodeProperties[key] === defaultProperties[key]) {
      return matches;
    }
    return matches && deepEqual(domProperties[key], vnodeProperties[key]);
  }, true);
}

function allMatches(dom, vnode) {
  const candidates = dom.querySelectorAll(extractName(vnode));
  const matches = [];
  for (let i = 0; i < candidates.length; i++) {
    if (propertiesMatch(candidates[i], vnode)) {
      matches.push(candidates[i]);
    }
  }
  return matches;
}

function domContains(dom, vnode) {
  return allMatches(dom, vnode).length > 0;
}

function matches(dom, vnode) {
  if (dom.nodeName.toLowerCase() === extractName(vnode).toLowerCase()) {
    return propertiesMatch(dom, vnode);
  }
  return false;
}

// Renders a shallow representation of the vnode into the DOM.
function shallowRender(preactEl, domEl) {
  // Override the `vnode` hook to transform composite components in the render
  // output into DOM elements.
  const oldVnodeHook = options.vnode;
  const vnodeHook = (node) => {
    if (oldVnodeHook) {
      oldVnodeHook(node);
    }
    if (typeof node.nodeName === 'string') {
      return;
    }
    node.nodeName = node.nodeName.name; // eslint-disable-line no-param-reassign
  };

  try {
    options.vnode = vnodeHook;
    const el = render(preactEl, domEl);
    options.vnode = oldVnodeHook;
    return el;
  } catch (err) {
    options.vnode = oldVnodeHook;
    throw err;
  }
}

// Primary interface for testing. The idea is that the vnode you supply will be used for property
// equality comparisons and non-provided properties are ignored. i.e. it is considered a match
// whenever any element in the DOM has at least the properties of the vnode.
export default function wrapper(preactEl) {
  const shallow = shallowRender(preactEl, document.createElement('div'));
  return {
    __element: shallow,
    contains: (vnode, matchExactly = false) => {
      return domContains(shallow, vnode);
    },
    is: (vnode, matchExactly = false) => {
      return matches(shallow, vnode);
    },
    find: (vnode) => {
      return allMatches(shallow, vnode);
    }
  };
}
