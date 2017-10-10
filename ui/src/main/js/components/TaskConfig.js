import React from 'react';

export default function TaskConfig({ config }) {
  return <pre>{JSON.stringify(config, null, 2)}</pre>;
}
