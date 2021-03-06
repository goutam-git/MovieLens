package com.okmich.movielens.model.dw

case class TagMovies(tag : String,
                      movies : Array[String]) {

  private val SEP = "|"
  override def toString: String = "[ "+this.tag + SEP + this.movies.mkString(SEP) + " ]"

}
