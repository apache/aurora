import React from 'react';

import { IconPanelTitle } from 'components/Layout';

import { statusDispatcher } from 'utils/Update';

const titleDispatch = {
  success: (update) => {
    return <IconPanelTitle className='success' icon='ok-sign' title='Update Successful' />;
  },
  warning: (update) => {
    return <IconPanelTitle className='attention' icon='warning-sign' title='Update Paused' />;
  },
  error: (update) => {
    return <IconPanelTitle className='error' icon='remove-sign' title='Update Failed' />;
  },
  inProgress: (update) => {
    return <IconPanelTitle className='highlight' icon='play-circle' title='Update In Progress' />;
  }
};

const titleDispatcher = statusDispatcher(titleDispatch);

export default function UpdateTitle({ update }) {
  return titleDispatcher(update);
}
