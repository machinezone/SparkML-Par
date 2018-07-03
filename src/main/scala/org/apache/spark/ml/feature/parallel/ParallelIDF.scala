/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.{IDF, IDFBase, IDFModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType

/**
  * ParallelIDFParams
  *
  * [[org.apache.spark.ml.param.Params]] for [[ParallelIDF]].
  * NOTE: because this inherits from [[IDFBase]] [[org.apache.spark.ml.param.shared.HasInputCol]]
  * is also inherited but will be ignored.
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelIDFParams extends IDFBase {

  override def validateAndTransformSchema(schema: StructType): StructType = {
    throw new RuntimeException("transformSchema will be implemented by ParallelPipelineStage.")
  }

}

/**
  * ParallelIDF
  *
  * [[IDF]] that handles multiple input columns in parallel.
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelIDF(override val uid: String) extends ParallelEstimator[ParallelIDFModel, IDFModel]
  with ParallelIDFParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelIDF"))

  /**
    * createStage
    *
    * Create a new instance of IDF
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): IDF = {
    new IDF()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
      .setMinDocFreq($(minDocFreq))
  }

  /**
    * createModel
    *
    * Create nwe instance of [[ParallelIDFModel]].
    *
    * @param models
    * @return
    */
  override def createModel(models: Map[String, IDFModel]): ParallelIDFModel = {
    new ParallelIDFModel(uid)
      .setInputCols($(inputCols))
      .setStages(models)
  }
}

/**
  * ParallelIDFModel
  *
  * [[Model]] that accepts multiple input columns
  *
  * @param uid
  */
class ParallelIDFModel(override val uid: String) extends Model[ParallelIDFModel]
  with ParallelModel[IDFModel] {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelIDFModel"))

}
