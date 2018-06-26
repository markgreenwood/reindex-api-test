const { expect } = require('chai');
const es = require('elasticsearch');
const R = require('ramda');

const {
  safeDeleteIndex,
  createNewIndex,
  compareIndicesContents,
  populateNewIndex,
  createUsers
} = require('./utils/test-utils');

const esClient = es.Client({ host: 'localhost:9200' });

const reindexSpec = (src, dest) => ({
  body: {
    source: {
      index: src
    },
    dest: {
      index: dest,
      version_type: 'external'
    }
  },
  refresh: true
});

describe('Reindex API', () => {
  context('when source index is reindexed to an empty destination index', () => {
    let sourceRecs;
    let destRecs;

    beforeEach(async () => {
      const userData = await createUsers(5);
      await createNewIndex({ esClient, index: 'test-source-index' });
      await createNewIndex({ esClient, index: 'test-dest-index' });
      await populateNewIndex({ esClient, index: 'test-source-index', data: userData });
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));
      [sourceRecs, destRecs] = await compareIndicesContents({ esClient })('test-source-index', 'test-dest-index');
    });

    it('will copy source documents to the destination index', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });

    afterEach(async () => Promise.all([
      safeDeleteIndex({ esClient, index: 'test-source-index' }),
      safeDeleteIndex({ esClient, index: 'test-dest-index' })
    ]));
  });

  context('when source index with altered record is reindexed onto existing destination index', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach(async () => {
      const userData = await createUsers(5);
      await createNewIndex({ esClient, index: 'test-source-index' });
      await createNewIndex({ esClient, index: 'test-dest-index' });
      await populateNewIndex({ esClient, index: 'test-source-index', data: userData });
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));

      let id = await esClient.search({
        index: 'test-source-index',
        type: '_doc',
        body: {
          query: {
            match: {
              name: 'Aisha Bonsaksen'
            }
          }
        }
      })
        .then(R.pipe(R.path(['hits', 'hits']), R.head, R.prop('_id')));

      await esClient.update({
        index: 'test-source-index',
        type: '_doc',
        id,
        body: { doc: { name: 'Aisha Newlastname' } },
        refresh: true
      });

      reindexResponse = await esClient.reindex(R.mergeDeepRight(reindexSpec('test-source-index', 'test-dest-index'), { body: { conflicts: 'proceed' } }));
      [sourceRecs, destRecs] = await compareIndicesContents({ esClient })('test-source-index', 'test-dest-index');
    });

    it('will only change the records that have changed since the first reindex', () => {
      const reindexStats = R.pick(['updated', 'deleted', 'created', 'version_conflicts']);
      expect(reindexStats(reindexResponse)).to.deep.equal({
        updated: 1,
        deleted: 0,
        created: 0,
        version_conflicts: 4
      });
    });

    it('will result in destination index that matches the source index', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });

    afterEach(async () => Promise.all([
      safeDeleteIndex({ esClient, index: 'test-source-index' }),
      safeDeleteIndex({ esClient, index: 'test-dest-index' })
    ]));
  });

  context('when source index with added record is reindexed onto destination index', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach('before source with added', async () => {
      let userData = await createUsers(5);
      await createNewIndex({ esClient, index: 'test-source-index' });
      await createNewIndex({ esClient, index: 'test-dest-index' });
      await populateNewIndex({ esClient, index: 'test-source-index', data: userData });
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));
      userData = await createUsers(6);
      await safeDeleteIndex({ esClient, index: 'test-source-index' });
      await createNewIndex({ esClient, index: 'test-source-index' });
      await populateNewIndex({ esClient, index: 'test-source-index', data: userData });
      reindexResponse = await esClient.reindex(R.mergeDeepRight(reindexSpec('test-source-index', 'test-dest-index'), { body: { conflicts: 'proceed' } }));
      [sourceRecs, destRecs] = await compareIndicesContents({ esClient })('test-source-index', 'test-dest-index');
    });

    it('will only change the records that have changed since the first reindex', () => {
      const reindexStats = R.pick(['updated', 'deleted', 'created', 'version_conflicts']);
      expect(reindexStats(reindexResponse)).to.deep.equal({
        updated: 0,
        deleted: 0,
        created: 1,
        version_conflicts: 5
      });
    });

    it('will only add to the destination the record that was added to the source', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });

    afterEach(async () => Promise.all([
      safeDeleteIndex({ esClient, index: 'test-source-index' }),
      safeDeleteIndex({ esClient, index: 'test-dest-index' })
    ]));
  });

  context('when source index is reindexed onto destination index with modified record', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach(async () => {
      let userData = await createUsers(5);
      await createNewIndex({ esClient, index: 'test-source-index' });
      await createNewIndex({ esClient, index: 'test-dest-index' });
      await populateNewIndex({ esClient, index: 'test-source-index', data: userData });
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));

      let id = await esClient.search({
        index: 'test-dest-index',
        type: '_doc',
        body: {
          query: {
            match: {
              name: 'Aisha Bonsaksen'
            }
          }
        }
      })
        .then(R.pipe(R.path(['hits', 'hits']), R.head, R.prop('_id')));

      await esClient.update({
        index: 'test-dest-index',
        type: '_doc',
        id,
        body: { doc: { name: 'Aisha Newlastname' } },
        refresh: true
      });

      reindexResponse = await esClient.reindex(R.mergeDeepRight(reindexSpec('test-source-index', 'test-dest-index'), { body: { conflicts: 'proceed' } }));
      [sourceRecs, destRecs] = await compareIndicesContents({ esClient })('test-source-index', 'test-dest-index');
    });

    it('will not overwrite the modified record', () => {
      const reindexStats = R.pick(['updated', 'deleted', 'created', 'version_conflicts']);
      expect(reindexStats(reindexResponse)).to.deep.equal({
        updated: 0,
        deleted: 0,
        created: 0,
        version_conflicts: 5
      });
    });
  });

  context('when source index is reindexed onto destination index with added record', () => {
    let sourceRecs, destRecs, reindexResponse;
    beforeEach(async () => {
      let userData = await createUsers(5);
      await createNewIndex({ esClient, index: 'test-source-index' });
      await createNewIndex({ esClient, index: 'test-dest-index' });
      await populateNewIndex({ esClient, index: 'test-source-index', data: userData });
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));
      userData = await createUsers(6);
      await safeDeleteIndex({ esClient, index: 'test-dest-index' });
      await createNewIndex({ esClient, index: 'test-dest-index' });
      await populateNewIndex({ esClient, index: 'test-dest-index', data: userData });
      reindexResponse = await esClient.reindex(R.mergeDeepRight(reindexSpec('test-source-index', 'test-dest-index'), { body: { conflicts: 'proceed' } }));
      [sourceRecs, destRecs] = await compareIndicesContents({ esClient })('test-source-index', 'test-dest-index');
    });

    it('will not remove the added record', () => {
      const reindexStats = R.pick(['updated', 'deleted', 'created', 'version_conflicts']);
      expect(reindexStats(reindexResponse)).to.deep.equal({
        updated: 0,
        deleted: 0,
        created: 0,
        version_conflicts: 5
      });
    });
  });
});
