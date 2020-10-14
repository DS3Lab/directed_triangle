package ch.ethz.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasIsDirected extends Params {
  /**
   * Param for isDirected.
   *
   * @group param
   */
  final val isDirected = new BooleanParam(this, "isDirected", "is directed graph or not")

  /** @group getParam */
  final def getIsDirected: Boolean = $(isDirected)

  setDefault(isDirected, false)

  final def setIsDirected(bool: Boolean): this.type = set(isDirected, bool)
}
