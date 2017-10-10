import createBuilder from '../Builder';

describe('createBuilder', () => {
  it('Should create a builder from a struct and allow me to chain mutators', () => {
    const builder = createBuilder({test: true, test2: 5});

    expect(builder.build()).toEqual({test: true, test2: 5});
    expect(builder.test(false).build()).toEqual({test: false, test2: 5});
    // original still intact
    expect(builder.build()).toEqual({test: true, test2: 5});
    // chain updates
    expect(builder.test(false).test2(10).build()).toEqual({test: false, test2: 10});
  });

  it('Should keep default values stable even after object is changed', () => {
    const test = {test: true, test2: 5};
    const builder = createBuilder(test);

    expect(builder.build()).toEqual({test: true, test2: 5});
    test.test = false;
    expect(builder.build()).toEqual({test: true, test2: 5});
  });

  it('Should not allow modifications to return values to modify the builder', () => {
    const original = {test: true, test2: 5};
    const builder = createBuilder(original);
    const result = builder.build();
    expect(result).toEqual(original);
    result.test = false;
    expect(builder.build()).toEqual(original);
  });
});
