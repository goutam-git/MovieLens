package com.okmich.movielens.jobs.dw

import com.okmich.movielens.jobs.JobTest
import com.okmich.movielens.model.da.Ratings
import com.okmich.movielens.model.dw.{MovieRatings, RatingsMovies}
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.mockito.Mockito
import org.mockito.Mockito.{doReturn, mock, withSettings}

class TransformRatingsMoviesTest   extends JobTest{
  implicit val ratingsEncoder : Encoder[Ratings] = Encoders.product[Ratings]
  implicit val movRatingsEncoder : Encoder[RatingsMovies] = Encoders.product[RatingsMovies]

  "TransformRatingsMovies" should "return a RatingsMovies Dataset by getting aggregate of ratings and timestamp grouped by movieId" in{
    val ratingsList : List[Ratings] = List(
       Ratings("1","2","3.5","1256677221"),
       Ratings("2","2","3.5","1256677456"))
    val ratingsDS : Dataset[Ratings] = createDataSet[Ratings](ratingsList)
    System.err.println("Ratings Dataset: " + printDataSet[Ratings](ratingsDS))

    val output : List[RatingsMovies] = List(
      RatingsMovies(2,3.5,7.0,2,1256677221,1256677456))

    val expectedOutput : Dataset[RatingsMovies] = createDataSet[RatingsMovies](output)
    System.err.println("Expected Output: " + printDataSet[RatingsMovies](expectedOutput))


    val job : TransFormRatingsMovies = new TransFormRatingsMovies(spark)
    val mockJob : TransFormRatingsMovies  = mock(classOf[TransFormRatingsMovies],withSettings().spiedInstance(job)
      .serializable.defaultAnswer(Mockito.CALLS_REAL_METHODS))
    doReturn(ratingsDS).when(mockJob).readCsv[Ratings]("testpath")

    val result :  Dataset[RatingsMovies]  = mockJob.transFormMovRatings(ratingsDS)
    System.err.println("RESULT: " + printDataSet[RatingsMovies](result))

    result.collect should contain theSameElementsAs expectedOutput.collect

    expectedOutput should beEqualToDataSet(result)
  }

}
