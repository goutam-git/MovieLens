package com.okmich.movielens.jobs.dw

import com.okmich.movielens.jobs.JobTest
import com.okmich.movielens.model.da.{Ratings, Tags}
import com.okmich.movielens.model.dw.MovieTags
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.mockito.Mockito
import org.mockito.Mockito.{doReturn, mock, withSettings}

class TransFormTagsTest  extends JobTest{

  implicit val tagsEncoder: Encoder[Tags] = Encoders.product[Tags]
  implicit val movTagsEncoder: Encoder[MovieTags] = Encoders.product[MovieTags]

  "TransformTags" should "return a MovieTags dataset by extracting year,month,dayofmonth,dayofweek,hours,mins from timestamp" in {

    val tagsList : List[Tags] = List(
      Tags("14","11","epic","1443148538"),
      Tags("27","26","sci-fi","1440448094")
    )
    val tagsDS: Dataset[Tags] = createDataSet[Tags](tagsList)

    System.err.println("Tgas Dataset: " + printDataSet[Tags](tagsDS))

    val output : List[MovieTags] = List(
      MovieTags(14,11,"epic",2015,9,25,"6",8,5,"PM",1443148538),
      MovieTags(27,26,"sci-fi",2015,8,25,"3",1,58,"PM",1440448094)
    )

    val expectedOutput : Dataset[MovieTags] = createDataSet[MovieTags](output)

    System.err.println("Expected Output: " + printDataSet[MovieTags](expectedOutput))

    val job : TransFormTags = new TransFormTags(spark)
    val mockJob : TransFormTags  = mock(classOf[TransFormTags],withSettings().spiedInstance(job)
                                     .serializable.defaultAnswer(Mockito.CALLS_REAL_METHODS))
                                      doReturn(tagsDS).when(mockJob).readCsv[Tags]("testpath")

    val result :  Dataset[MovieTags]  = mockJob.transFormTags(tagsDS)
    System.err.println("RESULT: " + printDataSet[MovieTags](result))

    result.collect should contain theSameElementsAs expectedOutput.collect

    expectedOutput should beEqualToDataSet(result)

  }

}
