import React from 'react';

export default function Icon({ name }) {
  return <span className={`glyphicon glyphicon-${name}`} />;
}
