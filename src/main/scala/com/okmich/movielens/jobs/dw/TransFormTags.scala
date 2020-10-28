package com.okmich.movielens.jobs.dw


import com.okmich.movielens.model.da.Tags
import com.okmich.movielens.model.dw.MovieTags
import com.okmich.movielens.utils.DateUtils
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.{Logger, LoggerFactory}
import com.okmich.movielens.utils.SparkUtils.loadSpark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object TransFormTags extends MovieLensConfig {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val tagsFile : String = config.getString(s"spark.${source}.reference.tags")
  val ratings_tbl  :String= config.getString(s"spark.${source}.db.da.ref_da_tbl")
  val rating_trnsfrm_tbl :String = config.getString(s"spark.${source}.db.dw.ratings_dw_tbl")
  val fmt : DateTimeFormatter = DateTimeFormat forPattern "yyyy-mm-dd HH:mm:ss.SSS"

  def apply(spark: SparkSession) = new TransFormTags(spark)

  def main(args : Array[String]) : Unit = {
    val spark : SparkSession = loadSpark(environment, master, warehouse)
    Try(TransFormTags(spark).workFlow()) match {
       case Success(value) => {
        logger.info("Process Completed Sucessfuly")
        spark.stop()
      }
      case Failure(exception) => {
        exception.printStackTrace()
        spark.stop()
      }
    }
  }

}
class TransFormTags (spark: SparkSession) extends Serializable {

  import TransFormTags._
  import spark.implicits._

  def workFlow(): Unit = {
    val tagsDS : Dataset[Tags] = readCsv[Tags](tagsFile)
    val transFormTagsDS : Dataset[MovieTags] = transFormTags(tagsDS)
    transFormTagsDS.show
  }

  def transFormTags(tagsDS: Dataset[Tags]) : Dataset[MovieTags] = {
    val tagsDF : DataFrame = addDateTimeCols(tagsDS)
    tagsDF.as[MovieTags]
  }

  def addDateTimeCols(tagsDS: Dataset[Tags]): DataFrame  = {
    val ampmUDF : UserDefinedFunction = udf(tagsAMPM(_:String))
    val transFormTagsDF : DataFrame = tagsDS.withColumn("year",  year(from_unixtime(col("timestamp"))))
                                            .withColumn("month",month(from_unixtime(col("timestamp"))))
                                            .withColumn("dayofmonth",dayofmonth(from_unixtime(col("timestamp"))))
                                            .withColumn("dayofweek",dayofweek(from_unixtime(col("timestamp"))))
                                            .withColumn("hour",hour(from_unixtime(col("timestamp"))))
                                            .withColumn("minitue",minute(from_unixtime(col("timestamp"))))
                                            .withColumn("am_pm",ampmUDF(col("timestamp")))
                                            .withColumn("userId",col("userId").cast("Integer"))
                                            .withColumn("movieId",col("movieId").cast("Integer"))
                                            .withColumn("ts",col("timestamp").cast("BigInt"))
                                            .drop(col("timestamp"))

    val orderDF :DataFrame =  transFormTagsDF.select(col("userId"),col("movieId"),
                                                    col("tag"),col("year"),
                                                    col("month"),col("dayofmonth"),
                                                    col("dayofweek"),col("hour"),
                                                    col("minitue"),col("am_pm"),
                                                    col("ts")
                                                  )
    orderDF
  }

  def tagsAMPM(timeStampStr:String) : String = {
    val ts =  timeStampStr.toLong
    val am_pm :String  = DateUtils(ts)
    am_pm
  }

  def readCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .csv(path)
      .as[T]
  }

}