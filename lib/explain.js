function createExplain(scan, docs, options) {
  options = options || { }
  return { 
    executionStats: {
      nReturned: docs.length,
      executionTimeMillis: options.elapsed || -1,
      totalKeysExamined: 0, // displays 0 to indicate that this is query is not using an index.
      totalDocsExamined: scan.KeyCount,
    },
    s3: {
      IsTruncated: scan.IsTruncated || false, 
      KeyCount: scan.KeyCount, 
      MaxKeys: scan.MaxKeys, 
      NextContinuationToken: scan.NextContinuationToken || null,
      TimeMillis: options.elapsedS3 || -1
    }
  }
}

// function initScan() {
//   return {
//     IsTruncated: false, 
//     KeyCount: 0, 
//     MaxKeys: -1, 
//     NextContinuationToken: scan.NextContinuationToken || null,
//     TimeMillis: options.elapsedS3 || -1
//   }
// }

module.exports = exports = {
  createExplain
}