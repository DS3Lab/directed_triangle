# graph_algo

## algorithm configurations

algo: node_iter, node_iter_plus, edge_iter, edge_iter_plus, fast, fast_v2
sep: tab, space, comma

## example for undirected graph

hdfs_host='hdfs_host'

/path_to_spark/bin/spark-submit \
  --class ch.ethz.ml.graph.runner.UndirectedRunner \
  --master yarn --deploy-mode cluster \
  --name "TriangleCount" \
  --driver-memory 2g \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 2g \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -verbose:gc -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps" \
  graph-1.0-SNAPSHOT.jar mode:yarn-cluster \
  input:hdfs://${hdfs_host}:9000/path_to_graph/ \
  output:hdfs://${hdfs_host}:9000/path_to_model/ \
  cpDir:hdfs://${hdfs_host}:9000/path_to_cp/ \
  partitionNum:4 storageLevel:DISK_ONLY \
  algo:fast_v2 sep:tab header:false


## example for directe graph

hdfs_host='hdfs_host'

/path_to_spark/bin/spark-submit \
  --class ch.ethz.ml.graph.runner.DirectedRunner \
  --master yarn --deploy-mode cluster \
  --name "TriangleCount" \
  --driver-memory 2g \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 2g \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -verbose:gc -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps" \
  graph-1.0-SNAPSHOT.jar mode:yarn-cluster \
  input:hdfs://${hdfs_host}:9000/path_to_graph/ \
  output:hdfs://${hdfs_host}:9000/path_to_model/ \
  cpDir:hdfs://${hdfs_host}:9000/path_to_cp/ \
  partitionNum:4 storageLevel:DISK_ONLY \
  algo:fast_v2 sep:tab header:false