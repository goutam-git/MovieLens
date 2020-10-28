package com.okmich.movielens.utils

import com.okmich.movielens.model.conf.Environment
import com.okmich.movielens.model.conf.Environment.EnvironmentEnum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

object SparkUtils {

  def loadSparkWithCheckpoint(spark: SparkSession.Builder, checkpoint: Option[String]): SparkSession.Builder = {
    checkpoint
      .map(folder => spark.config("spark.sql.streaming.checkpointLocation", folder))
      .getOrElse(spark)
  }

  def loadSpark(environment: EnvironmentEnum,
                master: String,
                warehouse: String = null): SparkSession = {
    val base = SparkSession
      .builder()
      .master(master)
      .appName("MovieLensRuntime")
    if (environment.equals(Environment.LOCAL)) {
      base
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    } else {
      base
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .enableHiveSupport()
        .getOrCreate()
    }
  }

  def saveDSTable[T](ds: Dataset[T], schema: String, tbl: String): Unit = {
    ds.write.option("compression", "snappy").format("parquet")
      .mode(SaveMode.Append).insertInto(s"${schema}.${tbl}")
  }


}
