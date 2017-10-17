import React from 'react';

import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';
import TaskConfig from 'components/TaskConfig';
import UpdateDiff from 'components/UpdateDiff';

export default function UpdateConfig({ update }) {
  if (update.update.instructions.initialState.length > 0) {
    return <UpdateDiff update={update} />;
  }

  return (<Container>
    <PanelGroup noPadding title={<StandardPanelTitle title='Update Config' />}>
      <TaskConfig config={update.update.instructions.desiredState.task} />
    </PanelGroup>
  </Container>);
}
