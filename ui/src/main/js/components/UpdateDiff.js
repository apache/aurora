import React from 'react';

import Diff from 'components/Diff';
import PanelGroup, { Container, StandardPanelTitle } from 'components/Layout';

import { instanceRangeToString } from 'utils/Task';

export default class UpdateDiff extends React.Component {
  constructor(props) {
    super(props);
    this.state = {groupIdx: 0};
  }

  diffNavigation() {
    const that = this;
    const { initialState } = this.props.update.update.instructions;
    const currentGroup = initialState[this.state.groupIdx];
    if (initialState.length < 2) {
      return '';
    } else {
      const otherOptions = initialState.map((g, i) => i).filter((i) => i !== that.state.groupIdx);
      return (<div className='update-diff-picker'>
        Current Instances:
        <select onChange={(e) => this.setState({groupIdx: parseInt(e.target.value, 10)})}>
          <option key='current'>{instanceRangeToString(currentGroup.instances)}</option>
          {otherOptions.map((i) => (<option key={i} value={i}>
            {instanceRangeToString(initialState[i].instances)}
          </option>))}
        </select>
      </div>);
    }
  }

  render() {
    const { initialState, desiredState } = this.props.update.update.instructions;
    return (<Container>
      <PanelGroup noPadding title={<StandardPanelTitle title='Update Diff' />}>
        <div className='task-diff'>
          {this.diffNavigation()}
          <Diff left={initialState[this.state.groupIdx].task} right={desiredState.task} />
        </div>
      </PanelGroup>
    </Container>);
  }
}
