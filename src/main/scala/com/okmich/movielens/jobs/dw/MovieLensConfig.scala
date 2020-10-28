package com.okmich.movielens.jobs.dw

import com.okmich.movielens.model.conf.Environment
import com.okmich.movielens.model.conf.Environment.EnvironmentEnum
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark
import org.apache.spark.sql.{Dataset, Encoder}

trait MovieLensConfig {
  val environment  : EnvironmentEnum = Environment.fromString(System.getProperty("ENVIRONMENT", Environment.LOCAL.name))
  val config :Config  = ConfigFactory.load("movie").getConfig(environment.name)
  val master :String = config.getString("spark.master")
  val warehouse :String  = config.getString("spark.warehouse")
  val source :String = config.getString("spark.source")


}
