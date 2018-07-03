/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * ParallelStringIndexerTest
  *
  * Unit tests for [[ParallelStringIndexer]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelStringIndexerTest extends TestBase {

  "#transform()" should "transform a dataframe" in {
    val df = spark.createDataFrame(Seq(
      (0, "a", "b"),
      (1, "b", "c"),
      (2, "c", "b"),
      (3, "a", "c"),
      (4, "a", "a"),
      (5, "c", "c")
    )).toDF("_1", "_2", "_3")

    val psi = new ParallelStringIndexer()
      .setInputCols(Array("_2", "_3"))
      .setOutputCols(Array("_2_si", "_3_si"))
      .setIntermediateCache(true)

    val out = psi.fit(df).transform(df).collect

    // _2 index
    out(0)(3) should equal(0.0)
    out(1)(3) should equal(2.0)
    out(2)(3) should equal(1.0)
    out(3)(3) should equal(0.0)
    out(4)(3) should equal(0.0)
    out(5)(3) should equal(1.0)

    // _3 index
    out(0)(4) should equal(1.0)
    out(1)(4) should equal(0.0)
    out(2)(4) should equal(1.0)
    out(3)(4) should equal(0.0)
    out(4)(4) should equal(2.0)
    out(5)(4) should equal(0.0)

  }

  "#transformSchema" should "add string index columns" in {
    val testSchema = StructType(Seq(
      StructField("_1", DataTypes.StringType),
      StructField("_2", DataTypes.StringType)
    ))

    val psi = new ParallelStringIndexer()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_si", "_2_si"))
    val outSchema = psi.transformSchema(testSchema)

    val firstField = outSchema(2)
    firstField.dataType should equal(DataTypes.DoubleType)
    firstField.name should equal("_1_si")

    val secondField = outSchema(3)
    secondField.dataType should equal(DataTypes.DoubleType)
    secondField.name should equal("_2_si")
  }

}
