package com.okmich.movielens.jobs.dw


import com.okmich.movielens.model.da.{Movies, Tags}
import com.okmich.movielens.model.dw.TagMovies
import com.okmich.movielens.utils.SparkUtils.loadSpark
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object TransFormTagMovies extends MovieLensConfig{
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val tagsFile : String = config.getString(s"spark.${source}.reference.tags")
  val moviesFile :String = config.getString(s"spark.${source}.reference.movies")
  def apply(spark:SparkSession) = new TransFormTagMovies(spark)

  def main(args: Array[String]) = {
    val spark : SparkSession = loadSpark(environment,master,warehouse)
    Try(TransFormTagMovies(spark).workFlow()) match {
      case Success(value) => {
        logger.info("Process Completed Sucessfuly")
        spark.stop()
      }
      case Failure( exception) => {
        exception.printStackTrace();
        spark.stop()
      }
    }
  }


}

class TransFormTagMovies(spark: SparkSession) extends Serializable {

  import TransFormTagMovies._
  import spark.implicits._

  def workFlow() : Unit = {
    val tagsDS: Dataset[Tags] = readCsv[Tags](tagsFile)
    val moviesDS: Dataset[Movies] = readCsv[Movies](moviesFile)
    val transFormTagMoviesDF : DataFrame = transFormTagMovies(moviesDS,tagsDS)
    val tagMoviesDS : Dataset[TagMovies] = transFormTagMoviesDF.as[TagMovies]
    tagMoviesDS.show
    // Thread.sleep(1000000)// inspecting spark UI
  }

  def transFormTagMovies(moviesDS:Dataset[Movies],tagsDS: Dataset[Tags]) : DataFrame = {
    val tagMoviesDF : DataFrame = moviesDS.join(tagsDS,moviesDS("movieId")===tagsDS("movieId"),"inner")
      .drop("movieId").drop("genres")
      .drop("userId").drop("timestamp")
    val transFormTagMoviesDF  : DataFrame = tagMoviesDF.groupBy("tag").agg(collect_list("title").as("movies"))
    transFormTagMoviesDF
  }


  def readCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .csv(path)
      .as[T]
  }


}