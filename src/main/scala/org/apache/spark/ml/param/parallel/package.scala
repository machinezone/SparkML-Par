/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.param

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.shared.HasInputCols

/**
  * parallel
  *
  * package object containing parallel [[org.apache.spark.ml.param.Param]] traits.
  *
  * @author belbis
  * @since 1.0.0
  */
package object parallel {

  /**
    * HasOutputCols
    *
    * Trait for [[PipelineStage]] that has multiple output columns. See [[org.apache.spark.ml.param.shared]]
    * for similar traits.
    *
    * @author belbis
    * @since 1.0.0
    */
  private[ml] trait HasOutputCols extends Params {

    /**
      * Param for output column names.
      */
    final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "output " +
      "column names")

    /**
      * getOutputCols
      *
      * getter for output column names.
      *
      * @return
      */
    final def getOutputCols: Array[String] = $(outputCols)

  }

  /**
    * HasInputOutputCols
    *
    * Trait for [[PipelineStage]]s that have multiple input and output columns.
    *
    * @author belbis
    * @since 1.0.0
    */
  private[ml] trait HasInputOutputCols extends HasInputCols with HasOutputCols

}
