package com.okmich.movielens.jobs.dw

import java.sql.Timestamp

import com.okmich.movielens.model.da.Ratings
import com.okmich.movielens.model.dw.MovieRatings
import com.okmich.movielens.utils.DateUtils
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import com.okmich.movielens.utils.SparkUtils.loadSpark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object TransFormRatings extends MovieLensConfig {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val ratingsFile:String = config.getString(s"spark.${source}.reference.ratings")
  val schema :String = config.getString(s"spark.${source}.db.schema")
  val ratings_tbl  :String= config.getString(s"spark.${source}.db.da.ref_da_tbl")
  val rating_trnsfrm_tbl :String = config.getString(s"spark.${source}.db.dw.ratings_dw_tbl")
  val fmt : DateTimeFormatter = DateTimeFormat forPattern "yyyy-mm-dd HH:mm:ss.SSS"

  def apply(spark:SparkSession) = new TransFormRatings(spark)
  def main(args: Array[String]) : Unit = {
    val spark : SparkSession = loadSpark(environment, master, warehouse)
    Try(TransFormRatings(spark).workflow) match {
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
class TransFormRatings(spark: SparkSession) extends Serializable{

  import TransFormRatings._
  import spark.implicits._

  def workflow() : Unit = {
    val ratingsDS : Dataset[Ratings] = readCsv[Ratings](ratingsFile)
    val trnsFrmRatingDS : Dataset[MovieRatings] = transformRatings(ratingsDS);
    trnsFrmRatingDS.show()
  }

  def transformRatings(ratingDS:Dataset[Ratings]) : Dataset[MovieRatings] = {
    val ratingDF : DataFrame = addDateTimeCols(ratingDS)
    ratingDF.as[MovieRatings]
  }


  def addDateTimeCols(ratingsDS : Dataset[Ratings]) : DataFrame = {
    val ampmUDF : UserDefinedFunction = udf(ratingAMPM(_:String))
    val  transfrmRatingDF : DataFrame = ratingsDS.withColumn("year",  year(from_unixtime(col("timestamp"))))
                                     .withColumn("month",month(from_unixtime(col("timestamp"))))
                                     .withColumn("dayofmonth",dayofmonth(from_unixtime(col("timestamp"))))
                                     .withColumn("dayofweek",dayofweek(from_unixtime(col("timestamp"))))
                                     .withColumn("hour",hour(from_unixtime(col("timestamp"))))
                                     .withColumn("minitue",minute(from_unixtime(col("timestamp"))))
                                     .withColumn("am_pm",ampmUDF(col("timestamp")))
                                     .withColumn("userId",col("userId").cast("Integer"))
                                     .withColumn("movieId",col("movieId").cast("Integer"))
                                     .withColumn("rating",col("rating").cast("Double"))
                                     .withColumn("ts",col("timestamp").cast("BigInt"))
                                     .drop(col("timestamp"))


    val orderDF :DataFrame =  transfrmRatingDF.select(col("userId"),col("movieId"),
                                                      col("rating"),col("year"),
                                                      col("month"),col("dayofmonth"),
                                                      col("dayofweek"),col("hour"),
                                                      col("minitue"),col("am_pm"),
                                                      col("ts")
                                                     )

    orderDF
  }



  def ratingAMPM(timeStampStr:String) : String = {
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