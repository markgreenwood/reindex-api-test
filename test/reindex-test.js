const { expect } = require('chai');
const es = require('elasticsearch');
const axios = require('axios');
const R = require('ramda');
const fs = require('fs');

const esClient = es.Client({ host: 'localhost:9200' });

const safeDeleteIndex = async (index) => {
  if (await esClient.indices.exists({ index })) {
    return esClient.indices.delete({ index });
  }

  return Promise.resolve({});
};

const createNewIndex = async (index) => {
  await safeDeleteIndex(index);
  return esClient.indices.create({ index });
};

const capitalize = string => string.charAt(0).toUpperCase() + string.substr(1);
const makeFullName = user => R.join(' ', R.map(capitalize, [user.name.first, user.name.last]));

const createUsers = async () => {
  const bulkify = R.compose(
    R.flatten,
    R.map(item => [{ index: { _index: 'test-source-index', _type: '_doc' } }, item])
  );

  return axios({
    method: 'GET',
    url: 'http://randomuser.me/api/?results=5&seed=theSeed'
  })
    .then(R.path(['data', 'results']))
    .then(R.map(user => ({ name: makeFullName(user), email: user.email })))
    .then(response => {
      console.log(response);
      fs.writeFileSync('./users.json', JSON.stringify(response, null, 2));
      return response;
    })
    .then(bulkify);
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

const reindexSpec = {
  body: {
    source: {
      index: 'test-source-index'
    },
    dest: {
      index: 'test-dest-index',
      version_type: 'external'
    }
  },
  refresh: true
};

describe('Reindex API', () => {
  before(async () => {
    await createNewIndex('test-source-index');
    const userData = await createUsers();
    return esClient.bulk({ refresh: true, body: userData });
  });

  context('when starting the test', () => {
    let response;

    beforeEach(async () => {
      response = await esClient.search({
        index: 'test-source-index',
        type: '_doc',
        body: {
          query: {
            match_all: {}
          }
        }
      });
    });

    it('will sets up a source database with 5 records', () => {
      expect(response.hits.total).to.equal(5);
    });
  });

  context('when source index is reindexed to destination index initially', () => {
    let sourceRecs;
    let destRecs;

    beforeEach(async () => {
      await esClient.reindex(reindexSpec);
      [sourceRecs, destRecs] = await compareIndicesContents('test-source-index', 'test-dest-index');
    });

    it('will copy source documents to the destination index', () => {
      expect(sourceRecs).to.deep.equal(destRecs);
    });
  });

  context('when altered source index is reindexed onto existing destination index', () => {
    let sourceRecs, destRecs, reindexResponse;

    beforeEach(async () => {
      await safeDeleteIndex('test-dest-index');
      await esClient.reindex(reindexSpec);

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

      reindexResponse = await esClient.reindex(R.mergeDeepRight(reindexSpec, { body: { conflicts: 'proceed' } }));
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
  });

  after(async () => {
    return Promise.all([
      esClient.indices.delete({ index: 'test-source-index' }),
      esClient.indices.delete({ index: 'test-dest-index' })
    ]);
  });
});
