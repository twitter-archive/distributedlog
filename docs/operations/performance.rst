Performance Tuning
==================

We've found that the following settings have given us the best performance (please refer to :doc`/configuration/core` for the meaning of each setting):

DL Zookeeper Settings
---------------------

- *zkSessionTimeoutSeconds=1*: this depends on your network configuration, but shorter timeout are usually preferable.


DL Bookkeeper Settings
----------------------

- *bkcWriteTimeoutSeconds=2*
- *bkc.connectTimeoutMillis=200*
- *bkc.enableParallelRecoveryRead=true*
- *bkc.recoveryReadBatchSize=5*

DL Settings
-----------

- *bkc.writeRequestToChannelAsync=true*
- *rolling-interval=120*
- *maxLogSegmentBytes=2147483648*
- *compressionType=lz4*: this will add overhead since more CPU is needed to run the compression.
