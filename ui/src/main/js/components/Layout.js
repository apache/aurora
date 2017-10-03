import React from 'react';

function ContentPanel({ children }) {
  return <div className='content-panel'>{children}</div>;
}

export function StandardPanelTitle({ title }) {
  return <div className='content-panel-title'>{title}</div>;
}

export default function PanelGroup({ children, title }) {
  return (<div className='content-panel-group'>
    {title}
    {React.Children.map(children, (p) => <ContentPanel>{p}</ContentPanel>)}
  </div>);
}
