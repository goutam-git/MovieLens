package com.okmich.movielens.model.da

import java.sql.Timestamp

object Tags {
  lazy val USER_ID = 'userId.name
  lazy val TAG = 'tag.name

}

case class Tags(userId:String,movieId:String,tag:String,timestamp:Timestamp) {
  private val SEP = "|"
  override def toString : String = "[ "+this.userId + SEP + this.movieId + SEP + this.tag
                                                      SEP + this.timestamp + " ]"
}
