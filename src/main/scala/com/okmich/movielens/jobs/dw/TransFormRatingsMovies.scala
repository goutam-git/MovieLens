package com.okmich.movielens.jobs.dw




import com.okmich.movielens.model.da.{Ratings, Tags}
import com.okmich.movielens.model.dw.RatingsMovies
import com.okmich.movielens.utils.SparkUtils.loadSpark
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object TransFormRatingsMovies extends MovieLensConfig{
 @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val ratingsFile:String = config.getString(s"spark.${source}.reference.ratings")
  val schema :String = config.getString(s"spark.${source}.db.schema")
  val ratings_tbl  :String= config.getString(s"spark.${source}.db.da.ref_da_tbl")
  val mov_rating_trnsfrm_tbl :String = config.getString(s"spark.${source}.db.dw.mov_ratings_dw_tbl")
  val fmt : DateTimeFormatter = DateTimeFormat forPattern "yyyy-mm-dd HH:mm:ss.SSS"

  def apply(spark:SparkSession) = new TransFormRatingsMovies(spark)

  def main(args: Array[String]) : Unit = {
    val spark : SparkSession = loadSpark(environment, master, warehouse)
    Try(TransFormRatingsMovies(spark).workflow) match {
      case Success(value) => {
        logger.info("Process Completed Sucessfuly")
        spark.stop()
      }
      case Failure(exception) =>{
        exception.printStackTrace()
        spark.stop()
      }
    }
  }

}
class TransFormRatingsMovies (spark: SparkSession) extends Serializable{
  import TransFormRatingsMovies._
  import spark.implicits._

 def workflow(): Unit ={
   val ratingsDS: Dataset[Ratings] = readCsv[Ratings](ratingsFile)
   ratingsDS.show
   val movRatingsDS : Dataset[RatingsMovies] = transFormMovRatings(ratingsDS)
   movRatingsDS.show
 }
  def transFormMovRatings(ratingsDS: Dataset[Ratings]) : Dataset[RatingsMovies] = {
    val typeDF :DataFrame = ratingsDS.withColumn("rating",col("rating").cast("Double"))
                                     .withColumn("timestamp",col("timestamp").cast("Long"))
                                     .withColumn("movieId",col("movieId").cast("Integer"))
    val aggRatingsDF = typeDF.groupBy("movieId")
                                .agg(avg("rating").as("avg_rating"),
                                     sum("rating").as("total_rating"),
                                     count("rating").cast("Integer").as("no_rating"),
                                     min("timestamp").as("first_rated_ts"),
                                     max("timestamp").as("last_rated_ts")
                                )
    val ratingsMoviesDs: Dataset[RatingsMovies] = aggRatingsDF.as[RatingsMovies]
    ratingsMoviesDs
  }

  def readCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .csv(path)
      .as[T]
  }
}