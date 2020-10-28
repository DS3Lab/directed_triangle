package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.params._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

// reduction with index,
class FastUndirectedV4(override val uid: String) extends Transformer
  with HasInput with HasSrcNodeIdCol with HasDstNodeIdCol with HasIsDirected
  with HasPartitionNum with HasStorageLevel {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def this() = this(Identifiable.randomUID("TriangleCount"))

  setDefault(srcNodeIdCol, "src")
  setDefault(dstNodeIdCol, "dst")

  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    println(s"partition number: ${$(partitionNum)}")

    println(">>> generate neighbors from the dataset")
    val neighborStart = System.currentTimeMillis()
    val neighbors = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.flatMap { row =>
      Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0)))
    }.groupByKey($(partitionNum))
      .map { case (v, neighbor) => (v, neighbor.filter(_ != v).toArray.distinct.sortWith(_ < _)) }
      .persist($(storageLevel))
    val numVertices = neighbors.count()
    println(s"count of neighbor tables = $numVertices, number of partitions = ${neighbors.getNumPartitions}")
    val neighborEnd = System.currentTimeMillis()
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")
    println(s"samples of neighbor RDD:")
    neighbors.take(num = 5).foreach { case (src, neighbors) =>
      println(s"src = $src, neighbors = ${neighbors.mkString(",")}") }
    val neighborsSize = neighbors.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of neighbor tables: max=${neighborsSize.max}, min=${neighborsSize.min}," +
      s"cost ${System.currentTimeMillis() - neighborEnd} ms")

    println(">>> generate max vertex")
    val maxVertexStart = System.currentTimeMillis()
    val maxVertex = neighbors.mapPartitions { iter =>
      Iterator.single(iter.map(_._1).max)
    }.max()
    val maxVertexEnd = System.currentTimeMillis()
    println(s"max vertex = $maxVertex, generate max vertex cost ${maxVertexEnd - maxVertexStart} ms")

    println(">>> generate vertex degrees after reduction")
    val degreeStart = System.currentTimeMillis()
    val filterDegree = sc.broadcast(math.sqrt(numVertices))
    val largeDegrees = neighbors.flatMap { case (v, neighbors) =>
      val numLarge = neighbors.count(_ > v)
      if (numLarge > filterDegree.value)
        Iterator.single(v, numLarge)
      else
        Iterator.empty
    }
    largeDegrees.foreachPartition(_ => Unit)
    val degreeEnd = System.currentTimeMillis()
    println(s"generate degrees larger than ${filterDegree.value} cost ${degreeEnd - degreeStart} ms")
    println(s"samples of large degree RDD:")
    largeDegrees.take(num = 5).foreach { case (vid, degree) =>
      println(s"vid = $vid, degree = $degree") }
    val maxDegreeStart = System.currentTimeMillis()
    val maxDegree = largeDegrees.map(_._2).max()
    val maxDegreeEnd = System.currentTimeMillis()
    println(s"max large degree = $maxDegree, cost ${maxDegreeEnd - maxDegreeStart} ms")

    println(">>> generate vertex reindex")
    val reindexStart = System.currentTimeMillis()
    var curReindex = maxVertex
    val reindex = largeDegrees.collect().sortBy(_._2).map { case (vid, _) =>
      curReindex = curReindex + 1
      (vid, curReindex)
    }.toMap
    val bcReindex = sc.broadcast(reindex)
    val reindexEnd = System.currentTimeMillis()
    println(s"generate reindex cost ${reindexEnd - reindexStart} ms")
    println(s"reindex = ${reindex.toString()}")

    println(">>> reindex neighbor tables")
    val reindexNbStart = System.currentTimeMillis()
    val reindexNeighbors = neighbors.map { case (srcId, nbs) =>
      FastUndirectedV4.reindexNeighbors(srcId, nbs, bcReindex.value)
    }
    reindexNeighbors.foreachPartition(_ => Unit)
    val reindexNbEnd = System.currentTimeMillis()
    println(s"reindex neighbor tables cost ${reindexNbEnd - reindexNbStart} ms")
    reindexNeighbors.take(num = 5).foreach { case (src, neighbors) =>
      println(s"src = $src, neighbors = ${neighbors.mkString(",")}") }

    println(">>> load edges from joined neighbor tables")
    val edgeStart = System.currentTimeMillis()
    val partNum = $(partitionNum)
    val edge = reindexNeighbors.flatMap { case (v, nb) =>
      nb.flatMap { nid =>
          val pid = EdgePartition2D.getPartition(v, nid, partNum)
          Iterator.single((pid, (v, nid)))
      }
    }.partitionBy(new HashPartitioner(partNum))
      .map { case (_, (src, dst)) =>
        Edge(src, dst, null.asInstanceOf[Byte])
      }.persist($(storageLevel))
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

    println(">>> checkpoint filtered neighbor tables and edges")
    val cpStart = System.currentTimeMillis()
    reindexNeighbors.foreachPartition(_ => Unit)
    reindexNeighbors.checkpoint()
    edge.foreachPartition(_ => Unit)
    edge.checkpoint()
    val cpEnd = System.currentTimeMillis()
    println(s"checkpoint cost ${cpEnd - cpStart} ms")
    neighbors.unpersist()
    largeDegrees.unpersist()
    //reindex.destroy()

    println(">>> count triangles")
    val countStart = System.currentTimeMillis()
    val graph = Graph(reindexNeighbors, edge)
    val totalClosedTriangle = FastUndirectedV4.computeNumOfClosedTriangle(graph, $(storageLevel))
    println(s"numTriangle=$totalClosedTriangle")
    val countEnd = System.currentTimeMillis()
    println(s"count triangles cost ${countEnd - countStart} ms")

    val output = dataset
      .sparkSession
      .sparkContext
      .parallelize(Seq(totalClosedTriangle)).map { value =>
      Row(value)
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("TriangleNum", DoubleType, nullable = false)
    ))
  }
}


object FastUndirectedV4 {

  def filterNeighborTable(vid: VertexId,
                          neighbors: Array[VertexId],
                          degrees: Map[VertexId, Int]): Array[VertexId] = {
    val ret = new ArrayBuffer[VertexId]()
    val minDegree = degrees.getOrElse(vid, 0)
    neighbors.foreach { nid =>
      val nDegree = degrees.getOrElse(nid, 0)
      if (nDegree > minDegree)
        ret.append(nid)
      else if (nDegree == minDegree && nid > vid)
        ret.append(nid)
    }
    ret.toArray
  }

  def reindexNeighbors(srcId: VertexId,
                       neighbors: Array[VertexId],
                       reindex: Map[VertexId, VertexId]): (VertexId, Array[VertexId]) = {
    val retNbs = new ArrayBuffer[VertexId]()
    if (reindex.contains(srcId)) {
      val reSrcId = reindex(srcId)
      neighbors.foreach { dstId =>
        if (reindex.contains(dstId)) {
          val reDstId = reindex(dstId)
          if (reDstId > reSrcId) {
            retNbs.append(reDstId)
          }
        } else if (dstId > reSrcId) {
          retNbs.append(dstId)
        }
      }
      (reSrcId, retNbs.toArray.sorted)
    } else {
      neighbors.foreach { dstId =>
        if (reindex.contains(dstId)) {
          val reDstId = reindex(dstId)
          if (reDstId > srcId)
            retNbs.append(reDstId)
        } else if (dstId > srcId) {
          retNbs.append(dstId)
        }
      }
      (srcId, retNbs.toArray.sorted)
    }
  }

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[VertexId], Byte],
                                 storageLevel: StorageLevel): Double = {
    graphWithAdj.triplets.mapPartitionsWithIndex { case (partId, iter) =>
      var count = 0L
      var numTriplets = 0
      iter.foreach { edgeTriplet =>
        if (null != edgeTriplet.srcAttr && null != edgeTriplet.dstAttr) {
          val minDegree = Math.min(edgeTriplet.srcAttr.length, edgeTriplet.dstAttr.length)
          val maxDegree = Math.max(edgeTriplet.srcAttr.length, edgeTriplet.dstAttr.length)
          if (minDegree >= 1 && maxDegree >= 2) {
            var i = 0
            var j = 0
            while (i < edgeTriplet.srcAttr.length && j < edgeTriplet.dstAttr.length) {
              if (edgeTriplet.srcAttr(i) == edgeTriplet.dstAttr(j)) {
                count += 1
                i += 1
                j += 1
              } else if (edgeTriplet.srcAttr(i) > edgeTriplet.dstAttr(j)) {
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
      println(s">>> triangle counts in partition $partId = $count")
      Iterator.single(count)
    }.sum()
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}
