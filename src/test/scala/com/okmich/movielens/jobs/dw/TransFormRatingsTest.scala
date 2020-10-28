package com.okmich.movielens.jobs.dw

import java.time.Instant

import com.okmich.movielens.jobs.JobTest
import com.okmich.movielens.model.da.{Movies, Ratings}
import com.okmich.movielens.model.dw.MovieRatings
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.mockito.Mockito
import org.mockito.Mockito.{doReturn, mock, withSettings}

class TransFormRatingsTest  extends JobTest{

  val now: Long = Instant.now().getEpochSecond();
  implicit val ratingsEncoder: Encoder[Ratings] = Encoders.product[Ratings]
  implicit val movRatingEncoder: Encoder[MovieRatings] = Encoders.product[MovieRatings]

  "TransformRatings" should "return a MovieRating dataset by extracting day,dayofmonth, hours,mins from  timestamp" in {
    val ratingsList : List[Ratings] = List(
      Ratings("1","2","3.5",now.toString),
      Ratings("2","3","4.5",now.toString)
    )
    val ratingsDS : Dataset[Ratings] = createDataSet[Ratings](ratingsList)
    System.err.println("Ratings Dataset: " + printDataSet[Ratings](ratingsDS))

    val output : List[MovieRatings] = List(
      MovieRatings(1,2,3.5,2020,8,21,"6",16,9,"PM",now),
      MovieRatings(2,3,4.5,2020,8,21,"6",16,9,"PM",now)
    )
    val expectedOutput : Dataset[MovieRatings] = createDataSet[MovieRatings](output)

    System.err.println("Expected Output: " + printDataSet[MovieRatings](expectedOutput))

    val job : TransFormRatings = new TransFormRatings(spark)
    val mockJob : TransFormRatings  = mock(classOf[TransFormRatings],withSettings().spiedInstance(job)
                                              .serializable.defaultAnswer(Mockito.CALLS_REAL_METHODS))
    doReturn(ratingsDS).when(mockJob).readCsv[Ratings]("testpath")

    val result :  Dataset[MovieRatings]  = mockJob.transformRatings(ratingsDS)
    System.err.println("RESULT: " + printDataSet[MovieRatings](result))

    result.collect should contain theSameElementsAs expectedOutput.collect

    expectedOutput should beEqualToDataSet(result)
  }

}
