import React from 'react';

import { expect } from 'chai';

import shallow from '../ShallowRender';

class Leaf extends React.Component {
  render() {
    return <div>Leaf</div>;
  }
}

class Node extends React.Component {
  render() {
    return <div><Leaf {...this.props} /> <span /> <div>Something Else</div></div>;
  }
}

class ThinWrapper extends React.Component {
  render() {
    return <Leaf {...this.props} />;
  }
}

class List extends React.Component {
  render() {
    return <ul><li><Leaf /></li><li><Leaf /></li></ul>;
  }
}

class GeneratedList extends React.Component {
  render() {
    return (<div><ul>{this.props.names.map((i) => <Leaf name={i} />)}</ul></div>);
  }
}

describe('shallow::contains', () => {
  it('Should respect shallow rendering', () => {
    expect(shallow(<Node />).contains(<Leaf />)).to.be.true;
  });

  it('Should handle multiple elements', () => {
    const el = shallow(<div><Node name='jon' /><Node name='dany' /></div>);
    expect(el.contains(<Leaf name='jon' />)).to.be.true;
    expect(el.contains(<Leaf name='dany' />)).to.be.true;
  });

  it('Should match properties based on target node', () => {
    const el = shallow(<Node name='jon' surname='snow' />);
    expect(el.contains(<Leaf name='jon' surname='snow' />)).to.be.true;
    expect(el.contains(<Leaf name='jon' />)).to.be.true;
    expect(el.contains(<Leaf surname='snow' />)).to.be.true;
    expect(el.contains(<Leaf />)).to.be.true;
    expect(el.contains(<Leaf name='jon' surname='snow'>jon snow</Leaf>)).to.be.false;
  });

  it('Should match children with text', () => {
    expect(shallow(<Node />).contains(<div>Something Else</div>)).to.be.true;
    expect(shallow(<Node />).contains(<div>Not Present</div>)).to.be.false;
  });

  it('Should work with deeply nested tree', () => {
    expect(shallow(<List />).contains(<li><Leaf /></li>)).to.be.true;
    expect(shallow(<List />).contains(<Leaf />)).to.be.true;
  });

  it('Should respect ordering of nested items', () => {
    const generated = shallow(<GeneratedList names={['jon', 'dany']} />);
    expect(generated.contains(<ul><Leaf name='jon' /><Leaf name='dany' /></ul>)).to.be.true;
    expect(generated.contains(<ul><Leaf name='dany' /></ul>)).to.be.true;
    expect(generated.contains(<ul><Leaf name='dany' /><Leaf name='jon' /></ul>)).to.be.false;
  });
});

describe('shallow::is', () => {
  it('Should handle standard HTML elements', () => {
    expect(shallow(<ThinWrapper />).is(<Leaf />)).to.be.true;
  });

  it('Should handle lists', () => {
    expect(shallow(<List />).is(<ul />)).to.be.true;
    expect(shallow(<List />).is(<ul><li><Leaf /></li><li><Leaf /></li></ul>)).to.be.true;
    expect(shallow(<List />)
      .is(<ul><li><Leaf /></li><li><Leaf /></li><li><Leaf /></li></ul>)).to.be.false;
  });
});
