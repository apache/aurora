import React from 'react';

import { addClass } from 'utils/Common';

function ContentPanel({ children }) {
  return <div className='content-panel'>{children}</div>;
}

export function StandardPanelTitle({ title }) {
  return <div className='content-panel-title'>{title}</div>;
}

export default function PanelGroup({ children, title, noPadding }) {
  const extraClass = noPadding ? ' content-panel-fluid' : '';
  return (<div className={addClass('content-panel-group', extraClass)}>
    {title}
    {React.Children.map(children, (p) => <ContentPanel>{p}</ContentPanel>)}
  </div>);
}

export function PanelRow({ children }) {
  return (<div className='flex-row'>
    {children}
  </div>);
}

export function Container({ children, className }) {
  const width = 12 / children.length;
  return (<div className={addClass('container', className)}>
    <div className='row'>
      {React.Children.map(children, (c) => <div className={`col-md-${width}`}>{c}</div>)}
    </div>
  </div>);
}
