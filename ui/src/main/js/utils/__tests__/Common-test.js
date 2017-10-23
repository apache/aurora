import { pluralize } from '../Common';

describe('pluralize', () => {
  it('Should treat empty lists as plural (e.g. zero tasks are...)', () => {
    expect(pluralize([], 'task')).toBe('tasks');
  });

  it('Should treat lists with multiple as plural', () => {
    expect(pluralize([1, 2, 3], 'task')).toBe('tasks');
  });

  it('Should treat single element lists as singular', () => {
    expect(pluralize([1], 'task')).toBe('task');
  });

  it('Should allow you to set your own plural form', () => {
    expect(pluralize([1, 2], 'task', 'beetlejuice')).toBe('beetlejuice');
  });
});
