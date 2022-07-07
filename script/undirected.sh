hdfs_host='graph-master'

/home/jiangjia/devs/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
  --class ch.ethz.ml.graph.runner.UndirectedRunner \
  --master yarn --deploy-mode cluster \
  --name "TriangleCount" \
  --driver-memory 2g \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 2g \
  --conf spark.network.timeout=900000 \
  --conf spark.executor.heartbeatInterval=100000 \
  graph-1.0-SNAPSHOT.jar \
  algo:node_iter mode:yarn-cluster \
  input:hdfs://${hdfs_host}:9000/user/root/graph/LiveJournal-undirected/com-lj.ungraph.txt \
  output:hdfs://${hdfs_host}:9000/user/root/model/ \
  partitionNum:4 storageLevel:MEMORY_AND_DISK \
  sep:tab directed:false header:false cpDir:hdfs://${hdfs_host}:9000/user/root/cp/