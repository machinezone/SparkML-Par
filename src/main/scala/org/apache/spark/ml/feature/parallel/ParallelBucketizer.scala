/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelBucketizer
  *
  * Parallel version of [[Bucketizer]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelBucketizer(override val uid: String) extends Model[ParallelBucketizer]
  with ParallelModel[Bucketizer] {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelBucketizer"))

}
