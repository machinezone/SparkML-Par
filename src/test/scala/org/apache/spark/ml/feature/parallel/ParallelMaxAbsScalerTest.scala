/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * ParallelMaxAbsScalerTest
  *
  * Unit tests for [[ParallelMaxAbsScaler]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelMaxAbsScalerTest extends TestBase {

  "#transform()" should "transform a dataframe" in {
    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -8.0),
        Vectors.dense(1.0, 0.1, -8.0)),
      (1, Vectors.dense(2.0, 1.0, -4.0),
        Vectors.dense(2.0, 1.0, -4.0)),
      (2, Vectors.dense(4.0, 10.0, 8.0),
        Vectors.dense(4.0, 10.0, 8.0))
    )).toDF("_1", "_2", "_3")

    val scaler = new ParallelMaxAbsScaler()
      .setInputCols(Array("_2", "_3"))
      .setOutputCols(Array("_2_scaled", "_3_scaled"))

    val out = scaler.fit(dataFrame).transform(dataFrame)

    out.select("_2_scaled", "_3_scaled").show

  }

  "#transformSchema" should "transform a schema" in {
    val testSchema = StructType(Seq(
      StructField("_1", SQLDataTypes.VectorType),
      StructField("_2", SQLDataTypes.VectorType)))
    val scaler = new ParallelMaxAbsScaler()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_scaled", "_2_scaled"))
    val outSchema = scaler.transformSchema(testSchema)
    outSchema.fields.filter(_.name == "_1_scaled")
      .head.dataType should equal(SQLDataTypes.VectorType)
    outSchema.fields.filter(_.name == "_2_scaled")
      .head.dataType should equal(SQLDataTypes.VectorType)
  }

}
