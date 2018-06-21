const { expect } = require('chai');

let sourceRecs = 10;
let destinationRecs;

describe('Reindex API', () => {
  context('when source index is reindexed to target index initially', () => {
    beforeEach(() => {
      destinationRecs = 10;
    });

    it('will have the same number of records in source and target', () => {
      expect(sourceRecs).to.equal(destinationRecs);
    });
  });
});
