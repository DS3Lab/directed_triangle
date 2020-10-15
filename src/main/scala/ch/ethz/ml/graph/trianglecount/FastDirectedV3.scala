package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.{EdgeType, TriangleType}
import ch.ethz.ml.graph.params._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, TripletFields, VertexId}
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

class FastDirectedV3(override val uid: String) extends Transformer
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
    // only store dst > src
    val neighbors = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.flatMap { row =>
      val src = row.getLong(0)
      val dst = row.getLong(1)
      Iterator((src, (dst, EdgeType.OUT)), (dst, (src, EdgeType.IN)))
    }.groupByKey(partNum)
      .map { case (v, neighbors) =>
        (v, neighbors.filter(_._1 > v).groupBy(_._1).flatMap { case (b, flags) =>
          Iterator.single((b, EdgeType.fromFlag(flags.map(_._2).toArray))) }.toArray.sortBy(_._1)
        )
      }.persist($(storageLevel))
    println(s"count of neighbor tables = ${neighbors.count()}, number of partitions = ${neighbors.getNumPartitions}")
    val neighborEnd = System.currentTimeMillis()
    println(s"samples of neighbor RDD:")
    neighbors.take(num = 10).foreach { case (src, neighbors) =>
      println(s"src = $src, neighbors = ${neighbors.mkString(",")}")}
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")

    println(">>> load edges from the neighbors RDD")
    val edgeStart = System.currentTimeMillis()
    val edge = neighbors.flatMap{ case (src, neighbors) =>
      FastDirectedV3.getEdgesWithType2(src, neighbors).map { row =>
        val pid = EdgePartition2D.getPartition(row._1, row._2, partNum)
        (pid, (row._1, row._2, row._3))
      }
    }.partitionBy(new HashPartitioner(partNum))
      .map { case (_, (src, dst, edgeType)) => Edge(src, dst, edgeType) }
      .persist($(storageLevel))
    edge.foreachPartition(_ => Unit)
    println(s"count of edge = ${edge.count()}, number of partitions = ${edge.getNumPartitions}")
    val edgeEnd = System.currentTimeMillis()
    println(s"generate edges cost ${edgeEnd - edgeStart} ms")
    println(s"samples of edge RDD:")
    edge.take(num = 2).foreach { edge =>
      println(s"src = ${edge.srcId}, dst = ${edge.dstId}, edge type = ${edge.attr}") }
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
    neighbors.foreachPartition(_ => Unit)
    neighbors.checkpoint()
    edge.foreachPartition(_ => Unit)
    edge.checkpoint()
    val cpEnd = System.currentTimeMillis()
    println(s"checkpoint cost ${cpEnd - cpStart} ms")

    println(">>> count triangles")
    val countStart = System.currentTimeMillis()
    val graph = Graph(neighbors, edge)
    val triangleCounts = FastDirectedV3.computeNumOfClosedTriangle(graph, $(storageLevel))
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

object FastDirectedV3 {

  // output Edge[], require OUT and BI tags
  def getEdgesWithType(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[Edge[Byte]] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.single(Edge(dst, src, EdgeType.OUT))
      else
        Iterator.single(Edge(src, dst, eType))
    }.toIterator
  }

  // output triple (src,dst,tag), require OUT and BI tags
  def getEdgesWithType2(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[(VertexId, VertexId, Byte)] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.single((dst, src, EdgeType.OUT))
      else
        Iterator.single((src, dst, eType))
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

    def msgCount(edgeType: Byte, srcAttr: Array[(VertexId, Byte)], dstAttr: Array[(VertexId, Byte)]): Array[Long] = {
      print(s"edge type = $edgeType, src neighbors = ${srcAttr.map(_._1).mkString(",")}, " +
        s"dst neighbors = ${dstAttr.map(_._1).mkString(",")}")
      val count = new Array[Long](7)
      if (null != srcAttr && null != dstAttr) {
        var i = 0
        var j = 0
        while (i < srcAttr.length && j < dstAttr.length) {
          if (srcAttr(i)._1 == dstAttr(j)._1) {
            val triType = TriangleType.toTypeEdgeIter(edgeType, srcAttr(i)._2, dstAttr(j)._2)
            count(triType.toInt) += 1
            i += 1
            j += 1
          } else if (srcAttr(i)._1 > dstAttr(j)._1) {
            j += 1
          } else {
            i += 1
          }
        }
      }
      println(s"${count.mkString(",")}")
      count
    }

    val count = graphWithAdj.aggregateMessages[ ( Array[Long], (Byte, Array[(VertexId, Byte)], Array[(VertexId, Byte)]) ) ] (
      triplet => {
        println(s"src vertex = ${triplet.srcId}, dst vertex = ${triplet.dstId}, edge type = ${triplet.attr}, " +
          s"dst neighbors = ${triplet.dstAttr.map(_._1).mkString(",")}")
        triplet.sendToDst(new Array[Long](7), (triplet.attr, triplet.srcAttr, triplet.dstAttr))
      },

      (a, b) => ( msgCount(a._2._1, a._2._2, a._2._3)
        .zip(msgCount(b._2._1, b._2._2, b._2._3))
        .map( x => x._1 + x._2), (0x00, Array.empty[(VertexId, Byte)], Array.empty[(VertexId, Byte)]) ),
      tripletFields = TripletFields.All
    ).map(_._2._1).reduce((c1, c2) => c1.zip(c2).map{ case (a, b) => a+b })

    count
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}








