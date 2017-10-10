import { clone } from 'utils/Common';

/**
 * Generates an immutable object builder from a base object. Each mutation
 * clones the builder and also populates fields with values present in the
 * original struct.
 *
 * Usage:
 *
 * const x = createBuilder({hello: 'world', test: true});
 * x.hello('universe').build(); // {hello: 'universe', test: true};
 */
export default function createBuilder(base) {
  function Builder() {
    this._entity = clone(base);
  }

  Object.keys(base).forEach((key) => {
    Builder.prototype[key] = function (value) {
      const updated = clone(this._entity);
      updated[key] = value;
      return createBuilder(updated);
    };
  });

  Builder.prototype.build = function () {
    return clone(this._entity);
  };

  return new Builder();
}
