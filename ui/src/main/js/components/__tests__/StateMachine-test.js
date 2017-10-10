import React from 'react';
import { shallow } from 'enzyme';

import StateMachine, { StateMachineToggle } from '../StateMachine';

describe('StateMachineToggle', () => {
  it('Should toggle the display state when clicked', () => {
    const states = [{
      state: 'One',
      timestamp: 0
    }, {
      state: 'Two',
      timestamp: 0
    }];

    const el = shallow(<StateMachineToggle states={states} toggleState={states[1]} />);
    expect(el.state().expanded).toBe(false);
    expect(el.contains(<StateMachine className={undefined} states={[states[1]]} />)).toBe(true);

    el.simulate('click');

    expect(el.state().expanded).toBe(true);
    expect(el.contains(<StateMachine className={undefined} states={states} />)).toBe(true);
  });
});
