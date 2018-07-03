/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerBase, StringIndexerModel}
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelStringIndexerParams
  *
  * [[org.apache.spark.ml.param.Params]] to use for [[ParallelStringIndexer]].
  *
  * NOTE: because this extends
  * [[StringIndexerBase]] there is [[org.apache.spark.ml.param.shared.HasInputCol]] and
  * [[org.apache.spark.ml.param.shared.HasOutputCol]] are inherited but will be ignored.
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelStringIndexerParams extends StringIndexerBase

/**
  * ParallelStringIndexer
  *
  * [[org.apache.spark.ml.feature.StringIndexer]] that handles multiple input columns.
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelStringIndexer(override val uid: String)
  extends ParallelEstimator[ParallelStringIndexerModel, StringIndexerModel]
    with ParallelStringIndexerParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelStringIndexer"))

  /**
    * createStage
    *
    * Create new instance of [[StringIndexer]] to be used on the input column.
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): StringIndexer = {
    new StringIndexer()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
      .setHandleInvalid($(handleInvalid))
  }

  /**
    * createModel
    *
    * Create new instance of [[ParallelStringIndexerModel]].
    *
    * @param models
    * @return
    */
  override def createModel(models: Map[String, StringIndexerModel]): ParallelStringIndexerModel = {
    new ParallelStringIndexerModel(uid)
      .setInputCols($(inputCols))
      .setStages(models)
  }

}

/**
  * ParallelStringIndexerModel
  *
  * [[Model]] output from [[ParallelStringIndexer]]. Used for indexing on multiple columns.
  *
  * @param uid
  * @author belbis
  * @since 1.0.0
  */
class ParallelStringIndexerModel(override val uid: String) extends Model[ParallelStringIndexerModel]
  with ParallelModel[StringIndexerModel] with ParallelStringIndexerParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelStringIndexerModel"))

}
