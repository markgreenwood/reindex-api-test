const R = require('ramda');
const axios = require('axios');
const fs = require('fs');

let users = require('../../users.json');

const safeDeleteIndex = async ({ esClient, index }) => {
  const exists = await esClient.indices.exists({ index });
  if (exists) {
    return esClient.indices.delete({ index });
  }
  return Promise.resolve(0);
};

const createNewIndex = async ({ esClient, index }) => {
  await safeDeleteIndex({ esClient, index });
  return esClient.indices.create({ index });
};

const populateNewIndex = async ({ esClient, index, data }) => {
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

const compareIndicesContents = ({ esClient }) => (index1, index2) => {
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

module.exports = {
  safeDeleteIndex,
  createNewIndex,
  createUsers,
  populateNewIndex,
  compareIndicesContents
};
