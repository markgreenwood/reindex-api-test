/*
version_type: 'external'
Preserves version from source, create missing docs, update docs whose destination
version is older than source. Setting this to 'internal' (or default) will blindly
overwrite all docs in the destination index.

op_type: 'create'
This will create missing docs in the destination index but all existing docs will create
a version conflict.

conflicts: 'proceed'
By default, conflicts abort the reindex, but conflicts: 'proceed' will just count them
and continue with other docs.

You can add a query to the source to only copy a subset of the docs from the source.
*/

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

const reindex = ({ esClient, srcIndex, destIndex }) => esClient.reindex(reindexSpec(srcIndex, destIndex));

module.exports = {
  reindex
};
