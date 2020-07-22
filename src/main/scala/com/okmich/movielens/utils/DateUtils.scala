package com.okmich.movielens.utils

import java.util.Calendar

object DateUtils {
  lazy val YEAR = 'year.name
  lazy val MONTH = 'month.name
  lazy val DAY_OF_MONTH = 'day_of_month.name
  lazy val DAY_OF_WEEK = 'day_of_week.name
  lazy val HOUR_OF_DAY = 'hour.name
  lazy val MINUTE = 'minute.name
  lazy val AM_PM = 'am_pm.name

  def apply(timestamp: Long,typ:String) : Int = {
    val dateUtil =  new DateUtils(timestamp)
    val dateValue = typ match {
      case YEAR => dateUtil.getYear
      case MONTH => dateUtil.getMonth
      case DAY_OF_MONTH  => dateUtil.getDayOfMonth
      case HOUR_OF_DAY => dateUtil.getHour
      case MINUTE => dateUtil.getMinute
    }
    dateValue
  }

  def apply(timestamp: Long) : String = new DateUtils(timestamp).getAMPM

}
class DateUtils(timeStamp:Long) {

   private val calender :Calendar  = Calendar.getInstance()
   calender.setTimeInMillis(timeStamp)

   def getYear:Int = calender.get(Calendar.YEAR)
   def getMonth:Int = calender.get(Calendar.MONTH)
   def getDayOfMonth : Int = calender.get(Calendar.DAY_OF_MONTH)
   def getDayOfWeek : Int = calender.get(Calendar.DAY_OF_WEEK)
   def getHour: Int = calender.get(Calendar.HOUR_OF_DAY)
   def getMinute: Int = calender.get(Calendar.MINUTE)
   def getAMPM : String =  if(calender.get(Calendar.AM_PM) == Calendar.AM ) "AM" else  "PM"

}