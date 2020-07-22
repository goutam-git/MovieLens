name := "movielens"
organization := "com.okmich.movielens"
version := "0.1-SNAPSHOT"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.8",
  test in assembly := {},
  assemblyJarName in assembly := s"${organization.value}-${name.value}-${version.value}.jar",
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> "jackson25.@1").inAll,
    ShadeRule.rename("org.joda.time.**" -> "jodatime29.@1").inAll,
    ShadeRule.rename("org.joda.convert.**" -> "jodaconvert29.@1").inAll
  ),
  fork in Test := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),

  libraryDependencies ++= Seq(
    "com.typesafe" % "config" % "1.3.2",
    "org.apache.spark" %% "spark-core" % "2.3.0.cloudera2" % "compile", // change to provided in deployment
    "org.apache.spark" %% "spark-hive" % "2.3.0.cloudera2" % "compile", // change to provided in deployment
    "org.apache.spark" %% "spark-sql" % "2.3.0.cloudera2" % "compile",  // change to provided in deployment
    "org.json4s" %% "json4s-native" % "3.2.11"
  )


)


val coreDependencies = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
 // "org.mockito" % "mockito-scala-scalatest_2.11" % "1.14.8" % "test",
  "org.apache.hadoop" % "hadoop-core" % "2.6.0-mr1-cdh5.13.3" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % "test"
).map(_.exclude("org.mortbay.jetty", "*")
  .exclude("javax.servlet", "*"))


lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
   libraryDependencies ++= coreDependencies
  )
  

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}