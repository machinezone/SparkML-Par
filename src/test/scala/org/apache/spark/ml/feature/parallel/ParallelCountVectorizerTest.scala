/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.linalg.{SQLDataTypes, Vector => MLVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

/**
  * ParallelCountVectorizerTest
  *
  * Unit tests for [[ParallelCountVectorizer]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelCountVectorizerTest extends TestBase {

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

    val pcv = new ParallelCountVectorizer()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_cv", "_2_cv"))

    val out = pcv.fit(df).transform(df).collect

    // ((6,[0,1,2,3],[2.0,1.0,1.0,2.0]))
    val cmp1 = Array(0.0, 0.0, 1.0, 1.0, 2.0, 2.0)
    out(0).getAs[MLVector](2).toArray.sorted should equal(cmp1)

    // (9,[0,2,4,6,7,8],[1.0,1.0,1.0,1.0,1.0,1.0]))
    val cmp2 = Array(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    out(0).getAs[MLVector](3).toArray.sorted should equal(cmp2)
  }

  "#transformSchema" should "add vector columns" in {
    val pcv = new ParallelCountVectorizer()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_1", "_2_2"))
    val outSchema = pcv.transformSchema(testSchema)

    val firstField = outSchema(2)
    firstField.dataType should equal(SQLDataTypes.VectorType)
    firstField.name should equal("_1_1")

    val secondField = outSchema(3)
    secondField.dataType should equal(SQLDataTypes.VectorType)
    secondField.name should equal("_2_2")
  }

}
