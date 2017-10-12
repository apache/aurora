import React from 'react';
import { Link } from 'react-router-dom';

function url(...args) {
  return args.join('/');
}

export default function Breadcrumb({ cluster, role, env, name, instance, update }) {
  const crumbs = [<Link key='cluster' to='/beta/scheduler'>{cluster}</Link>];
  if (role) {
    crumbs.push(<span key='role-divider'>/</span>);
    crumbs.push(<Link key='role' to={`/beta/scheduler/${url(role)}`}>{role}</Link>);
  }
  if (env) {
    crumbs.push(<span key='env-divider'>/</span>);
    crumbs.push(<Link key='env' to={`/beta/scheduler/${url(role, env)}`}>{env}</Link>);
  }
  if (name) {
    crumbs.push(<span key='name-divider'>/</span>);
    crumbs.push(<Link key='name' to={`/beta/scheduler/${url(role, env, name)}`}>{name}</Link>);
  }
  if (instance) {
    crumbs.push(<span key='instance-divider'>/</span>);
    crumbs.push(<Link key='instance' to={`/beta/scheduler/${url(role, env, name, instance)}`}>
      {instance}
    </Link>);
  }
  if (update) {
    crumbs.push(<span key='update-divider'>/</span>);
    crumbs.push(<Link key='update' to={`/beta/scheduler/${url(role, env, name, 'update', update)}`}>
      {update}
    </Link>);
  }
  return (<div className='aurora-breadcrumb'>
    <div className='container'>
      <h2>{crumbs}</h2>
    </div>
  </div>);
}
