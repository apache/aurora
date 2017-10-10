import React from 'react';

import { Container, ContentPanel, StandardPanelTitle, PanelSubtitle } from 'components/Layout';
import UpdateInstanceEvents from 'components/UpdateInstanceEvents';
import UpdateInstanceSummary from 'components/UpdateInstanceSummary';
import UpdateSettings from 'components/UpdateSettings';
import UpdateStateMachine from 'components/UpdateStateMachine';
import UpdateStatus from 'components/UpdateStatus';
import UpdateTitle from 'components/UpdateTitle';

export default function UpdateDetails({ update }) {
  return (<Container>
    <div className='content-panel-group'>
      <UpdateTitle update={update} />
      <ContentPanel><UpdateStatus update={update} /></ContentPanel>
      <PanelSubtitle title='Update History' />
      <ContentPanel><UpdateStateMachine update={update} /></ContentPanel>
      <PanelSubtitle title='Update Settings' />
      <ContentPanel><UpdateSettings update={update} /></ContentPanel>
    </div>
    <div className='content-panel-group'>
      <StandardPanelTitle title='Instance Overview' />
      <ContentPanel><UpdateInstanceSummary update={update} /></ContentPanel>
      <PanelSubtitle title='Instance Events' />
      <div className='content-panel fluid'><UpdateInstanceEvents update={update} /></div>
    </div>
  </Container>);
}
