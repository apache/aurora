import { instanceRangeToString } from '../Task';

function range(first, last) {
  return {first, last};
}

describe('instanceRangeToString', () => {
  it('Should handle single instance ranges', () => {
    expect(instanceRangeToString([range(0, 0)])).toBe('0');
  });

  it('Should handle multiple single instance ranges', () => {
    expect(instanceRangeToString([range(0, 0), range(1, 1), range(2, 2)])).toBe('0, 1, 2');
  });

  it('Should handle instance ranges', () => {
    expect(instanceRangeToString([range(0, 5), range(6, 6), range(7, 7)])).toBe('0 - 5, 6, 7');
  });
});
