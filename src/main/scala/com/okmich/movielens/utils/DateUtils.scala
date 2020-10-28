package com.okmich.movielens.utils

import java.text.SimpleDateFormat
import java.util.Calendar

object DateUtils {
  lazy val YEAR = 'year.name
  lazy val MONTH = 'month.name
  lazy val DAY_OF_MONTH = 'day_of_month.name
  lazy val DAY_OF_WEEK = 'day_of_week.name
  lazy val HOUR_OF_DAY = 'hour.name
  lazy val MINUTE = 'minute.name
  lazy val AM_PM = 'am_pm.name


  def apply(timestamp: Long) : String = new DateUtils(timestamp).getAMPM

}
class DateUtils(timeStamp:Long) {

   private val calender :Calendar  = Calendar.getInstance()
   calender.setTimeInMillis(timeStamp)
   val sdf : SimpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.SSS")
   def getAMPM : String =  if(calender.get(Calendar.AM_PM) == Calendar.AM ) "AM" else  "PM"

}