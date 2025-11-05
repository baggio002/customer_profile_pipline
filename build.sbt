val scala2Version = "2.13.17"
val sparkVersion = "3.5.7"

lazy val root = project
  .in(file("."))
  .settings(
    name := "customer-segments-metrics",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala2Version,

    libraryDependencies ++= Seq("org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.43.1",
      "org.apache.hadoop" % "hadoop-client" % "3.4.2",
      "org.postgresql" % "postgresql" % "42.7.8"
    )
  )
