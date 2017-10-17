import React from 'react';

import Diff from 'components/Diff';

import { instanceRangeToString } from 'utils/Task';

export default class ConfigDiff extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      leftGroupIdx: 0,
      rightGroupIdx: 1
    };
  }

  getPicker(key) {
    const that = this;
    const group = this.props.groups[this.state[key]];
    if (this.props.groups.length === 2) {
      return (<span>
        Instances {instanceRangeToString(group.instances)}
      </span>);
    } else {
      const otherOptions = this.props.groups
        .map((g, i) => i)
        .filter((i) => i !== that.state.leftGroupIdx && i !== that.state.rightGroupIdx);
      return (<span>
        Instances <select onChange={(e) => this.setState({[key]: parseInt(e.target.value, 10)})}>
          <option key='current'>{instanceRangeToString(group.instances)}</option>
          {otherOptions.map((i) => (<option key={i} value={i}>
            {instanceRangeToString(this.props.groups[i].instances)}
          </option>))}
        </select>
      </span>);
    }
  }

  diffNavigation() {
    if (this.props.groups.length < 2) {
      return <div>No configuration.</div>;
    } else {
      return (<div className='diff-picker'>
        Config Diff for <span className='diff-before'>
          {this.getPicker('leftGroupIdx')}
        </span> and <span className='diff-after'>
          {this.getPicker('rightGroupIdx')}
        </span>
      </div>);
    }
  }

  render() {
    if (this.props.groups.length < 2) {
      return <div />;
    }

    return (<div className='task-diff'>
      {this.diffNavigation()}
      <Diff
        left={this.props.groups[this.state.leftGroupIdx].config}
        right={this.props.groups[this.state.rightGroupIdx].config} />
    </div>);
  }
}
