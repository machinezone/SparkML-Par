/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

/**
  * ParallelHashingTFTest
  *
  * Unit tests for [[ParallelHashingTF]]
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelHashingTFTest extends TestBase {

  val testSchema = StructType(Seq(
    StructField("_1", ArrayType(DataTypes.StringType)),
    StructField("_2", ArrayType(DataTypes.StringType))
  ))

  "#transform()" should "transform a dataframe" in {
    val df = spark.createDataFrame(spark.sparkContext
      .parallelize(Seq(
        Row(
          Array("all", "you", "need", "all", "love", "you"),
          Array("whether", "'tis", "nobler", "in", "the", "mind")
        ),
        Row(
          Array("need", "you", "is", "one", "love"),
          Array("that", "is", "the", "question"),
          Row(
            Array("california", "love"),
            Array("to", "be", "or", "not", "to", "be")
          )
        ))), testSchema)

    val pohe = new ParallelHashingTF()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_11", "_22"))

    val out = pohe.transform(df).collect

    val cmp1 = "(262144,[83161,135560,186480,252801],[1.0,2.0,1.0,2.0])"
    out(0)(2).toString should equal(cmp1)
    val cmp2 = "(262144,[87367,103838,149608,170688,222453,234007],[1.0,1.0,1.0,1.0,1.0,1.0])"
    out(0)(3).toString should equal(cmp2)
  }

  "#transformSchema" should "add columns" in {
    val pcv = new ParallelHashingTF()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_htf", "_2_htf"))
    val outSchema = pcv.transformSchema(testSchema)

    val firstField = outSchema(2)
    firstField.dataType should equal(SQLDataTypes.VectorType)
    firstField.name should equal("_1_htf")

    val secondField = outSchema(3)
    secondField.dataType should equal(SQLDataTypes.VectorType)
    secondField.name should equal("_2_htf")
  }

}
