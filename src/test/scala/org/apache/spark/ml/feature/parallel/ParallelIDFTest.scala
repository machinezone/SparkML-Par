/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors, Vector => MLVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * ParallelIDFTest
  *
  * Unit tests for [[ParallelIDF]]
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelIDFTest extends TestBase {

  val testSchema = StructType(Seq(
    StructField("_1", SQLDataTypes.VectorType),
    StructField("_2", SQLDataTypes.VectorType)
  ))

  "#transform()" should "transform a vector" in {
    val df = spark.createDataFrame(spark.sparkContext
      .parallelize(Seq(
        Row(
          Vectors.dense(Array(3.0, 5.0, 1.0, 1.0, 2.0, 2.0)),
          Vectors.dense(Array(3.0, 5.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
        )
      )), testSchema)

    val pidf = new ParallelIDF()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_idf", "_2_idf"))

    val out = pidf.fit(df).transform(df).collect

    // ((6,[0,1,2,3],[2.0,1.0,1.0,2.0]))
    val cmp1 = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    out(0).getAs[MLVector](2).toArray.sorted should equal(cmp1)

    // (9,[0,2,4,6,7,8],[1.0,1.0,1.0,1.0,1.0,1.0]))
    val cmp2 = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    out(0).getAs[MLVector](3).toArray.sorted should equal(cmp2)
  }

  "#transformSchema" should "add vector columns" in {
    val pidf = new ParallelIDF()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_idf", "_2_idf"))
    val outSchema = pidf.transformSchema(testSchema)

    val firstField = outSchema(2)
    firstField.dataType should equal(SQLDataTypes.VectorType)
    firstField.name should equal("_1_idf")

    val secondField = outSchema(3)
    secondField.dataType should equal(SQLDataTypes.VectorType)
    secondField.name should equal("_2_idf")

  }

}
