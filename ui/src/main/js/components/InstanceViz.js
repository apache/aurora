import React from 'react';
import { Link } from 'react-router-dom';

export default function InstanceViz({ instances, jobKey }) {
  const {job: {role, environment, name}} = jobKey;
  const className = (instances.length > 1000)
    ? 'small'
    : (instances.length > 100) ? 'medium' : 'big';

  return (<ul className={`instance-grid ${className}`}>
    {instances.map((i) => {
      return (<Link
        key={i.instanceId}
        to={`/beta/scheduler/${role}/${environment}/${name}/${i.instanceId}`}>
        <li className={i.className} title={i.title} />
      </Link>);
    })}
  </ul>);
}
