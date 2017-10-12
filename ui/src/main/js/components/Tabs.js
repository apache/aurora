import React from 'react';

import Icon from 'components/Icon';

import { addClass } from 'utils/Common';

export default class Tabs extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      active: props.activeTab || props.tabs[0].name
    };
  }

  select(name) {
    this.setState({active: name});
  }

  render() {
    const that = this;
    const isActive = (t) => t.name === that.state.active;
    return (<div className={addClass('tabs', this.props.className)}>
      <ul className='tab-navigation'>
        {this.props.tabs.map((t) => (
          <li
            className={isActive(t) ? 'active' : ''}
            key={t.name}
            onClick={(e) => this.select(t.name)}>
            {t.icon ? <Icon name={t.icon} /> : ''}
            {t.name}
          </li>))}
      </ul>
      <div className='active-tab'>
        {this.props.tabs.find(isActive).content}
      </div>
    </div>);
  }
}
