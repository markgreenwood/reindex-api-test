const { expect } = require('chai');
const es = require('elasticsearch');
const axios = require('axios');
const R = require('ramda');
const fs = require('fs');
let users = require('../users.json');

const esClient = es.Client({ host: 'localhost:9200' });

const safeDeleteIndex = async ({ index }) => {
  if (await esClient.indices.exists({ index })) {
    return esClient.indices.delete({ index });
  }
  return Promise.resolve(0);
};

const createNewIndex = async ({ index }) => {
  await safeDeleteIndex({ index });
  return esClient.indices.create({ index });
};

const populateNewIndex = async ({ index, data }) => {
  const mapIndexed = R.addIndex(R.map);

  const bulkify = R.compose(
    R.flatten,
    mapIndexed((item, idx) => [{ index: { _index: index, _type: '_doc', _id: idx } }, item])
  );

  await esClient.bulk({ refresh: true, body: bulkify(data) });
};

const capitalize = string => string.charAt(0).toUpperCase() + string.substr(1);
const makeFullName = user => R.join(' ', R.map(capitalize, [user.name.first, user.name.last]));

const createUsers = async (numUsers) => {
  if (!users) {
    users = await axios({
      method: 'GET',
      url: 'http://randomuser.me/api/?results=10&seed=theSeed'
    })
      .then(R.path(['data', 'results']))
      .then(R.map(user => ({ name: makeFullName(user), email: user.email })))
      .then(response => {
        console.log(response);
        fs.writeFileSync('./users.json', JSON.stringify(response, null, 2));
        return response;
      });
  }

  return R.take(numUsers, users);
};

const compareIndicesContents = (index1, index2) => {
  return Promise.all([
    esClient.search({
      index: index1,
      type: '_doc',
      body: {
        query: {
          match_all: {}
        }
      }
    }).then(R.path(['hits', 'hits'])),
    esClient.search({
      index: index2,
      type: '_doc',
      body: {
        query: {
          match_all: {}
        }
      }
    }).then(R.path(['hits', 'hits']))
  ])
    .then(R.map(R.map(R.omit(['_index']))));
};

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
  context('when starting the test', () => {
    let response;
    const index = 'test-source-index';
    const type = '_doc';

    beforeEach(async () => {
      await createNewIndex({ index: 'test-source-index' });
      const userData = await createUsers(5);
      await populateNewIndex({ index: 'test-source-index', data: userData });
      response = await esClient.search({ index, type, body: { query: { match_all: {} } } });
    });

    it('will sets up a source database with 5 records', () => {
      expect(response.hits.total).to.equal(5);
    });
  });

  context('when source index is reindexed to destination index initially', () => {
    let sourceRecs;
    let destRecs;

    beforeEach(async () => {
      await esClient.reindex(reindexSpec('test-source-index', 'test-dest-index'));
      [sourceRecs, destRecs] = await compareIndicesContents('test-source-index', 'test-dest-index');
    });

    it('will copy source documents to the destination index', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });

    afterEach(() => {
      safeDeleteIndex({ index: 'test-dest-index' });
    });
  });

  context('when source index with altered record is reindexed onto existing destination index', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach(async () => {
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
      [sourceRecs, destRecs] = await compareIndicesContents('test-source-index', 'test-dest-index');
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

    afterEach(() => {
      safeDeleteIndex({ index: 'test-dest-index' });
    });
  });

  context('when source index with added record is reindexed onto destination index', () => {
    let sourceRecs, destRecs;

    beforeEach(async () => {
      await createNewIndex({ index: 'test-source2-index' });
      const userData = await createUsers(6);
      await populateNewIndex({ index: 'test-source2-index', data: userData });
      await esClient.reindex(reindexSpec('test-source2-index', 'test-dest-index'));
      [sourceRecs, destRecs] = await compareIndicesContents('test-source2-index', 'test-dest-index');
    });

    it('will only add to the destination the record that was added to the source', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });

    afterEach(() => {
      safeDeleteIndex({ index: 'test-dest-index' });
    });
  })

  after(async () => {
    return Promise.all([
      // safeDeleteIndex({ index: 'test-source-index' }),
      // safeDeleteIndex({ index: 'test-source2-index' }),
      // safeDeleteIndex({ index: 'test-dest-index' })
    ]);
  });
});
