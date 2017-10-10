import React from 'react';

import Icon from 'components/Icon';

import { addClass } from 'utils/Common';

export function ContentPanel({ children }) {
  return <div className='content-panel'>{children}</div>;
}

export function StandardPanelTitle({ title }) {
  return <div className='content-panel-title'>{title}</div>;
}

export function PanelSubtitle({ title }) {
  return <div className='content-panel-subtitle'>{title}</div>;
}

export function IconPanelTitle({ title, className, icon }) {
  return (<div className={`content-icon-title ${className}`}>
    <div className='content-icon-title-text'>
      <Icon name={icon} /> {title}
    </div>
  </div>);
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
  const width = 12 / (children.length || 1);
  return (<div className={addClass('container', className)}>
    <div className='row'>
      {React.Children.map(children, (c) => <div className={`col-md-${width}`}>{c}</div>)}
    </div>
  </div>);
}
