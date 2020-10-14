package ch.ethz.ml.graph.ops

import ch.ethz.ml.graph.data.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object GraphOps {

  def loadEdges(dataset: Dataset[_],
                srcNodeIdCol: String,
                dstNodeIdCol: String
               ): RDD[(VertexId, VertexId)] = {
    dataset.select(srcNodeIdCol, dstNodeIdCol).rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else
          Iterator.single((row.getLong(0), row.getLong(1)))
      }
    }
  }

}
