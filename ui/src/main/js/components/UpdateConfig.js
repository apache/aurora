import React from 'react';

import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';
import TaskConfig from 'components/TaskConfig';

export default function UpdateConfig({ update }) {
  return (<Container>
    <PanelGroup noPadding title={<StandardPanelTitle title='Update Config' />}>
      <TaskConfig config={update.update.instructions.desiredState.task} />
    </PanelGroup>
  </Container>);
}
