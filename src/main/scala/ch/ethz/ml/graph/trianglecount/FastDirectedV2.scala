package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.{EdgeType, TriangleType, VertexId}
import ch.ethz.ml.graph.params._
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

class FastDirectedV2(override val uid: String) extends Transformer
  with HasInput with HasSrcNodeIdCol with HasDstNodeIdCol with HasIsDirected
  with HasPartitionNum with HasStorageLevel {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def this() = this(Identifiable.randomUID("TriangleCount"))

  setDefault(srcNodeIdCol, "src")
  setDefault(dstNodeIdCol, "dst")

  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    val partNum = $(partitionNum)

    println(">>> generate neighbors from the dataset")
    val neighborStart = System.currentTimeMillis()
    val neighbors = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.flatMap { row =>
      Iterator((row.getLong(0), (row.getLong(1), EdgeType.OUT)), (row.getLong(1), (row.getLong(0), EdgeType.IN)))
    }.groupByKey(partNum)
      .map { case (v, neighbors) =>
        (v, neighbors.toArray.distinct.filter(_._1 != v).groupBy(_._1).flatMap { case (b, flags) =>
          Iterator.single((b, EdgeType.fromFlag(flags.map(_._2)))) }.toArray.sortBy(_._1)
        )
      }.persist($(storageLevel))
    println(s"count of neighbor tables = ${neighbors.count()}, number of partitions = ${neighbors.getNumPartitions}")
    val neighborEnd = System.currentTimeMillis()
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")

    println(">>> generate vertex degrees")
    val degreeStart = System.currentTimeMillis()
    val degrees = neighbors.map { case (v, neighbors) =>
      (v, neighbors.length)
    }
    degrees.foreachPartition(_ => Unit)
    val degreeEnd = System.currentTimeMillis()
    println(s"generate degrees cost ${degreeEnd - degreeStart} ms")
    val maxDegreeStart = System.currentTimeMillis()
    val maxDegree = degrees.map(_._2).max()
    val maxDegreeEnd = System.currentTimeMillis()
    println(s"max degree = $maxDegree, cost ${maxDegreeEnd - maxDegreeStart} ms")
    val bcDegreeStart = System.currentTimeMillis()
    val bcDegrees = sc.broadcast(degrees.collectAsMap())
    val bcDegreeEnd = System.currentTimeMillis()
    println(s"broadcast degrees cost ${bcDegreeEnd - bcDegreeStart} ms")

    println(">>> filter neighbors with degree")
    val filterStart = System.currentTimeMillis()
    val filterNeighbors = neighbors.map { case (v, neighbors) =>
      (v, FastDirectedV2.filterNeighborTable(v, neighbors, bcDegrees.value))
    }.persist($(storageLevel))
    filterNeighbors.foreachPartition(_ => Unit)
    println(s"count of filter neighbors = ${filterNeighbors.count()}, number of partitions = ${filterNeighbors.getNumPartitions}")
    val filterEnd = System.currentTimeMillis()
    println(s"filter neighbors cost ${filterEnd - filterStart} ms")
    val filterNeighborsSize = filterNeighbors.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of filtered neighbor tables: max=${filterNeighborsSize.max}, min=${filterNeighborsSize.min}")
    val filterMaxDegree = filterNeighbors.map(_._2.length).max()
    println(s"max degree of filtered neighbors = $filterMaxDegree," +
      s"cost ${System.currentTimeMillis() - filterEnd} ms")

    import org.apache.spark.HashPartitioner
    def partitionBy[ED](edges: RDD[Edge[ED]], partitionStrategy: PartitionStrategy): RDD[Edge[ED]] = {
      val numPartitions = edges.partitions.size
      edges.map(e => (partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions), e))
        .partitionBy(new HashPartitioner(numPartitions))
        .mapPartitions(_.map(_._2), preservesPartitioning = true)
    }

    println(">>> load edges from the neighbors RDD")
    val edgeStart = System.currentTimeMillis()
    val edge = partitionBy(filterNeighbors.flatMap { case (src, filterNb) =>
      FastDirectedV2.getEdgesWithType(src, filterNb)
    }, PartitionStrategy.EdgePartition2D)
    edge.foreachPartition(_ => Unit)
    println(s"count of edge = ${edge.count()}, number of partitions = ${edge.getNumPartitions}")
    val edgeEnd = System.currentTimeMillis()
    println(s"generate edges cost ${edgeEnd - edgeStart} ms")
    val edgeSize = edge.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of edge: max=${edgeSize.max}, min=${edgeSize.min}," +
      s"cost ${System.currentTimeMillis() - edgeEnd} ms")

    println(">>> count edge tags")
    val countEdgeTagStart = System.currentTimeMillis()
    val outEdge = edge.filter(_.attr == 0x00)
    println(s"number of out edge = ${outEdge.count()}")
    val inEdge = edge.filter(_.attr == 0x01)
    println(s"number of in edge = ${inEdge.count()}")
    val biEdge = edge.filter(_.attr == 0x02)
    println(s"number of bi edge = ${biEdge.count()}")
    val countEdgeTagEnd = System.currentTimeMillis()
    println(s"count edge tags cost ${countEdgeTagEnd - countEdgeTagStart} ms")

    println(">>> checkpoint filtered neighbor tables and edges")
    val cpStart = System.currentTimeMillis()
    filterNeighbors.foreachPartition(_ => Unit)
    filterNeighbors.checkpoint()
    edge.foreachPartition(_ => Unit)
    edge.checkpoint()
    val cpEnd = System.currentTimeMillis()
    println(s"checkpoint cost ${cpEnd - cpStart} ms")
    neighbors.unpersist()
    degrees.unpersist()

    println(">>> count triangles")
    val countStart = System.currentTimeMillis()
    val graph = Graph(filterNeighbors, edge)
    val triangleCounts = FastDirectedV2.computeNumOfClosedTriangle(graph, $(storageLevel))
    println(s"numTriangle=${triangleCounts.mkString(",")}")
    val countEnd = System.currentTimeMillis()
    println(s"count triangles cost ${countEnd - countStart} ms")

    val output = dataset.sparkSession.sparkContext
      .parallelize(Seq(
      Row(triangleCounts(0), triangleCounts(1), triangleCounts(2), triangleCounts(3),
        triangleCounts(4), triangleCounts(5), triangleCounts(6))
      ))

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("Trans", LongType, nullable = false),
      StructField("OutRecip", LongType, nullable = false),
      StructField("InRecip", LongType, nullable = false),
      StructField("Cycle", LongType, nullable = false),
      StructField("OneRecip", LongType, nullable = false),
      StructField("TwoRecip", LongType, nullable = false),
      StructField("ThreeRecip", LongType, nullable = false)
    ))
  }
}


object FastDirectedV2 {

  // output Edge[], require OUT and BI tags
  def getEdgesWithType(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[Edge[Byte]] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.single(Edge(dst, src, EdgeType.OUT))
      else
        Iterator.single(Edge(src, dst, eType))
    }.toIterator
  }

  // output tuple (src,(dst,tag)), require OUT and BI tags
  def getEdgesWithType2(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[(VertexId, (VertexId, Byte))] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.single((dst, (src, EdgeType.OUT)))
      else
        Iterator.single((src, (dst, eType)))
    }.toIterator
  }

  // output triple (src,dst,tag), require OUT and BI (src<dst)
  def getEdgesWithType3(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[(VertexId, VertexId, Byte)] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.empty
      else if (eType == EdgeType.BI && src > dst)
        Iterator.empty
      else
        Iterator.single((src, dst, eType))
    }.toIterator
  }

  def filterNeighborTable(vid: VertexId,
                          neighbors: Array[(VertexId, Byte)],
                          degrees: Map[VertexId, Int]): Array[(VertexId, Byte)] = {
    val ret = new ArrayBuffer[(VertexId, Byte)]()
    val minDegree = degrees.getOrElse(vid, 0)
    neighbors.foreach { case (nid, tag) =>
      val nDegree = degrees.getOrElse(nid, 0)
      if (nDegree > minDegree)
        ret += ((nid, tag))
      else if (nDegree == minDegree && nid > vid)
        ret += ((nid, tag))
    }
    ret.toArray
  }

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[(VertexId, Byte)], Byte],
                                 storageLevel: StorageLevel): Array[VertexId] = {
    graphWithAdj.triplets.mapPartitionsWithIndex { case (partId, iter) =>
      val count = new Array[Long](7)
      var numTriplets = 0
      iter.foreach { edgeTriplet =>
        if (null != edgeTriplet.srcAttr && null != edgeTriplet.dstAttr) {
          val minDegree = Math.min(edgeTriplet.srcAttr.length, edgeTriplet.dstAttr.length)
          val maxDegree = Math.max(edgeTriplet.srcAttr.length, edgeTriplet.dstAttr.length)
          if (minDegree >= 1 && maxDegree >= 2) {
            var i = 0
            var j = 0
            while (i < edgeTriplet.srcAttr.length && j < edgeTriplet.dstAttr.length) {
              if (edgeTriplet.srcAttr(i)._1 == edgeTriplet.dstAttr(j)._1) {
                val triType = TriangleType.toTypeEdgeIter(edgeTriplet.attr, edgeTriplet.srcAttr(i)._2, edgeTriplet.dstAttr(j)._2)
                count(triType.toInt) += 1
                i += 1
                j += 1
              } else if (edgeTriplet.srcAttr(i)._1 > edgeTriplet.dstAttr(j)._1) {
                j += 1
              } else {
                i += 1
              }
            }
          }
        }
        numTriplets = numTriplets + 1
        if (numTriplets % 100000 == 0) println(s">>> $numTriplets triplets processed in partition $partId")
      }
      println(s">>> triangle counts in partition $partId = ${count.mkString(",")}")
      Iterator.single(count)
    }.reduce((c1, c2) => c1.zip(c2).map{ case (a, b) => a+b })
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}



