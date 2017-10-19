import React from 'react';

import Icon from 'components/Icon';

import { addClass } from 'utils/Common';

// Wrapping tabs in his component helps simplify testing of Tabs with enzyme's shallow renderer.
export function Tab({ children, icon, id, name }) {
  return <div>{children}</div>;
}

export default class Tabs extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      active: props.activeTab || React.Children.toArray(props.children)[0].props.id
    };
  }

  select(tab) {
    this.setState({active: tab.id});
    if (this.props.onChange) {
      this.props.onChange(tab);
    }
  }

  render() {
    const that = this;
    const isActive = (t) => t.id === that.state.active;
    return (<div className={addClass('tabs', this.props.className)}>
      <ul className='tab-navigation'>
        {React.Children.map(this.props.children, (t) => (<li
          className={isActive(t.props) ? 'active' : ''}
          key={t.props.name}
          onClick={(e) => this.select(t.props)}>
          {t.props.icon ? <Icon name={t.props.icon} /> : ''}
          {t.props.name}
        </li>))}
      </ul>
      <div className='active-tab'>
        {React.Children.toArray(this.props.children).find((t) => isActive(t.props))}
      </div>
    </div>);
  }
}
