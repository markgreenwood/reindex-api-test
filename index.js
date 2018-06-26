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
