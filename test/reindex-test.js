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

const { reindex } = require('../index.js');

const esClient = es.Client({ host: 'localhost:9200' });

const createIndices = () => Promise.all([
  createNewIndex({ esClient, index: 'test-source-index' }),
  createNewIndex({ esClient, index: 'test-dest-index' })
]);

const reindexSpec = (src, dest) => ({
  body: {
    source: {
      index: src
    },
    dest: {
      index: dest,
      version_type: 'external'
    },
    conflicts: 'proceed'
  },
  refresh: true
});

describe('Reindex API', () => {
  let userData;

  before(async () => {
    userData = await createUsers(6);
  });

  context('when source index is reindexed to an empty destination index', () => {
    let sourceRecs;
    let destRecs;

    beforeEach(async () => {
      await createIndices();
      await populateNewIndex({ esClient, index: 'test-source-index', data: R.take(5, userData) });
      await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });
      [sourceRecs, destRecs] = await compareIndicesContents({ esClient })('test-source-index', 'test-dest-index');
    });

    it('will copy source documents to the destination index', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });
  });

  context('when source index with altered record is reindexed onto existing destination index', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach(async () => {
      await createIndices();
      await populateNewIndex({ esClient, index: 'test-source-index', data: R.take(5, userData) });
      await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });

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

      reindexResponse = await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });
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
  });

  context('when source index with added record is reindexed onto destination index', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach('before source with added', async () => {
      await createIndices();
      await populateNewIndex({ esClient, index: 'test-source-index', data: R.take(5, userData) });
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));
      await esClient.index({ index: 'test-source-index', type: '_doc', id: '5', body: userData[5], refresh: true });
      reindexResponse = await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });
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
  });

  context('when source index is reindexed onto destination index with modified record', () => {
    let reindexResponse;

    beforeEach(async () => {
      await createIndices();
      await populateNewIndex({ esClient, index: 'test-source-index', data: R.take(5, userData) });
      await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });

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

      reindexResponse = await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });
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
    let reindexResponse;
    beforeEach(async () => {
      await createIndices();
      await populateNewIndex({ esClient, index: 'test-source-index', data: R.take(5, userData) });
      await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });
      await esClient.index({ index: 'test-dest-index', type: '_doc', id: '5', body: userData[5], refresh: true });
      reindexResponse = await reindex({ esClient, srcIndex: 'test-source-index', destIndex: 'test-dest-index' });
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

  after(async () => Promise.all([
    safeDeleteIndex({ esClient, index: 'test-source-index' }),
    safeDeleteIndex({ esClient, index: 'test-dest-index' })
  ]));
});
