const R = require('ramda');
const config = require('config');
const es = require('elasticsearch');
const AWS = require('aws-sdk');
const httpAwsEs = require('http-aws-es');

const users = require('../users');
const { populateNewIndex, createNewIndex } = require('./utils/test-utils');

AWS.config.update(config.get('aws'));
const esClient = new es.Client({ host: config.get('elasticsearch').host, connectionClass: httpAwsEs });

console.log(users);

(async () => {
  // Create the source index and populate it with data
  await populateNewIndex({ esClient, index: 'fakeusers-source-index', data: users });

  // Assign 'in' and 'out' aliases to the source index
  await esClient.indices.putAlias({ index: 'fakeusers-source-index', name: 'fakeusers-in' });
  await esClient.indices.putAlias({ index: 'fakeusers-source-index', name: 'fakeusers-out' });

  let response = await esClient.search({
    index: 'fakeusers-out',
    size: 100
  });
  console.log(`Got ${response.hits.hits.length} records.`);
  console.log(R.map(R.path(['_source', 'name']), response.hits.hits));

  // Create a new target index
  await createNewIndex({ esClient, index: 'fakeusers-target-index' });

  // Add the 'out' alias to the new index
  await esClient.indices.putAlias({ index: 'fakeusers-target-index', name: 'fakeusers-out' });

  // Move the 'in' alias to the new index
  await esClient.indices.updateAliases({
    body: {
      actions: [
        { remove: { index: 'fakeusers-source-index', alias: 'fakeusers-in' } },
        { add: { index: 'fakeusers-target-index', alias: 'fakeusers-in' } }
      ]
    }
  });

  // Write a new record to the target index
  await esClient.index({
    index: 'fakeusers-in',
    type: '_doc',
    body: { name: 'Mark Greenwood', email: 'mark.greenwood@example.com' }
  });

  // Grab a record from the source index and modify it, writing it to the target index
  response = await esClient.get({ index: 'fakeusers-source-index', type: '_doc', id: '1' });
  const body = R.prop('_source', response);
  body.name = 'Ayeesha Bonsaksen';
  console.log(body);
  await esClient.index({ index: 'fakeusers-in', type: '_doc', id: '1', body })

  // Reindex the source onto the target
  await esClient.reindex({
    body: {
      source: {
        index: 'fakeusers-source-index'
      },
      dest: {
        index: 'fakeusers-target-index',
        version_type: 'external'
      },
      conflicts: 'proceed'
    },
    refresh: true
  });

  // Remove the 'out' index from the source
  await esClient.indices.updateAliases({
    body: {
      actions: [
        { remove: { index: 'fakeusers-source-index', alias: 'fakeusers-out' } }
      ]
    }
  });
})();
