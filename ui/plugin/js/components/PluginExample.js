// This is an example of how to replace components with your own custom UI logic.
import React from 'react';

// You can import the original OSS component if you'd just like to wrap it.
import OSSPluginExample from '../../../src/main/js/components/PluginExample';

// Add your own CSS by importing files directly
import './plugin.css';

// And then be sure to match the interface of the file you're injecting. For Home, we just
// need to make sure we export a component by default.
export default function () {
  return <div className='custom-home'><OSSPluginExample /> and my own internal logic!</div>;
}
