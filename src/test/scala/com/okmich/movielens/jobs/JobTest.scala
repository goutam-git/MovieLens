package com.okmich.movielens.jobs

import java.util
import java.util.regex.Pattern

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

abstract class JobTest extends FlatSpec with Matchers
                                        with DataFrameSuiteBase
                                        with DatasetSuiteBase {

  private class EqualToDataFrameMatcher(df: DataFrame) extends Matcher[DataFrame] {
    override def apply(left: DataFrame): MatchResult = {
      val result = Try {
        assertDataFrameEquals(df, left)
      }.isSuccess

      MatchResult(result, s"$df and $left are not similar", s"$df and $left are similar")
    }
  }

  private class EqualToDataSetMatcher[T <: Product : ClassTag : TypeTag](ds: Dataset[T]) extends Matcher[Dataset[T]] {
    override def apply(left: Dataset[T]): MatchResult = {
      val result = Try {
        assertDatasetEquals[T](ds, left)
      }.isSuccess

      MatchResult(result, s"$ds and $left are not similar", s"$ds and $left are similar")
    }
  }

  private class ApproxEqualToDataFrameMatcher(df: DataFrame, precision: Double) extends Matcher[DataFrame] {
    override def apply(left: DataFrame): MatchResult = {
      val result = Try {
        assertDataFrameApproximateEquals(df, left, precision)
      }.isSuccess

      MatchResult(result, s"$df and $left are not similar", s"$df and $left are similar")
    }
  }


  private class ApproxEqualToDatasetMatcher[T <: Product : ClassTag : TypeTag](ds: Dataset[T], precision: Double) extends Matcher[Dataset[T]] {
    override def apply(left: Dataset[T]): MatchResult = {
      val result = Try {
        assertDatasetApproximateEquals[T](ds, left, precision)
      }.isSuccess

      MatchResult(result, s"$ds and $left are not similar", s"$ds and $left are similar")
    }
  }

  def beEqualToDataFrame(df: DataFrame): Matcher[DataFrame] = new EqualToDataFrameMatcher(df)

  def beEqualToDataSet[T <: Product : ClassTag : TypeTag](ds: Dataset[T]) : Matcher[Dataset[T]] = new EqualToDataSetMatcher[T](ds)

  def beApproxEqualToDataFrame(df: DataFrame, precision: Double): Matcher[DataFrame] = new ApproxEqualToDataFrameMatcher(df, precision)

  def beApproxEqualToDataSet[T <: Product : ClassTag : TypeTag](ds: Dataset[T], precision: Double): Matcher[Dataset[T]] = new ApproxEqualToDatasetMatcher[T](ds, precision)

  /**
    * Method used to create a DataFrame
    *
    * @param table Content of the DataFrame
    * @tparam A case class used to represent a row of the DataFrame
    * @return DataFrame formed from all the parameter case classes passed to it
    */
  protected def createDataFrame[A <: Product : ClassTag : TypeTag](table: List[A]): DataFrame = {
    createDataFrame(table: _*)
  }

  /**
    * Method used to create a DataFrame
    *
    * @param table Content of the DataFrame
    * @tparam A case class used to represent a row of the DataFrame
    * @return DataFrame formed from all the parameter case classes passed to it
    */
  protected def createDataSet[A <: Product : ClassTag : TypeTag](table: List[A])
                               (implicit  encoder: Encoder[A]): Dataset[A] = {
    createDataSet[A](table: _*)
  }

  /**
    * Method used to create a DataFrame
    *
    * @param rows Content of the DataFrame
    * @tparam A case class used to represent a row of the DataFrame
    * @return DataFrame formed from all the parameter case classes passed to it
    */
  protected def createDataFrame[A <: Product : ClassTag : TypeTag](rows: A*): DataFrame = {

    val rdd = sc.parallelize(rows)
    sqlContext.createDataFrame(rdd)
  }



  /**
    * Method used to create a DataSet
    *
    * @param rows Content of the DataSet
    * @tparam A case class used to represent a row of the DataSet
    * @return DataSet formed from all the parameter case classes passed to it
    */
  protected def createDataSet[A <: Product : ClassTag : TypeTag](rows: A*)
           (implicit  encoder: Encoder[A]): Dataset[A] = {

    val rdd = sc.parallelize(rows)
    sqlContext.createDataset[A](rdd)
  }

  /**
    * Utility method used for debugging purposes. Exports the structure and the content of a DataFrame to a String
    *
    * @param df DataFrame for which you want the String representation
    * @return String with the schema of the DataFrame and its content
    */
  implicit def printDataFrame(df: DataFrame): String = {
    val ln = System.lineSeparator()
    val schema = df.toString() + ln

    df.collect()
      .map { row =>
        row.mkString("[ ", " | ", " ]")
      }
      .mkString(schema, ln, "")
  }


  /**
    * Utility method used for debugging purposes. Exports the structure and the content of a DataFrame to a String
    *
    * @param ds DataFrame for which you want the String representation
    * @return String with the schema of the DataFrame and its content
    */
  implicit def printDataSet[T](ds: Dataset[T]): String = {
    val ln = System.lineSeparator()
    val schema = ds.toString() + ln
     ds.collect()
      .map{ row =>
         row.toString

      }
      .mkString(schema, ln, "")
  }



  /**
    * Method used to retrieve all parameters within a given String. Useful to create parameter Map objects for tests
    * (Extracted from https://stackoverflow.com/questions/5705111/how-to-get-all-substring-for-a-given-regex)
    * @param str input String
    * @return Seq[String] that contains all parameter keys within the String. Returns an empty Seq if there are no parameters.
    */
  protected def getParamsInString(str: String): Seq[String] = {
    import scala.collection.JavaConverters._

    val matches = new util.ArrayList[String]()
    val m = Pattern.compile("(?=${([^}]*)})").matcher(str)
    while (m.find()) {
      matches.add(m.group(1))
    }

    matches.asScala
  }


}
