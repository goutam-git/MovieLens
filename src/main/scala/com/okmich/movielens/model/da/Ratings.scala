package com.okmich.movielens.model.da

import java.sql.Timestamp
import java.text.SimpleDateFormat


object Ratings {
  lazy val RATING = 'rating.name
}

case class Ratings(userId:String,
                   movieId:String,
                   rating:String,
                   timestamp:String){
  private val SEP = "|"
  override def toString : String = "[ "+this.userId + SEP + this.movieId + SEP + this.rating +
                                                      SEP + this.timestamp + " ]"



}
