package ch.ethz.ml.graph.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasIsHeader extends Params {
  /**
   * Param for isHeader.
   *
   * @group param
   */
  final val isHeader = new BooleanParam(this, "isHeader", "is the input file having header")

  /** @group getParam */
  final def getIsHeader: Boolean = $(isHeader)

  setDefault(isHeader, false)

  final def setIsHeader(bool: Boolean): this.type = set(isHeader, bool)
}
