ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-hive-to-clickhouse-loader",
    version := "0.1.0",
    // Spark should be provided by cluster/runtime
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided,
      "org.apache.spark" %% "spark-hive" % "3.3.2" % Provided,
      "com.clickhouse" % "clickhouse-jdbc" % "0.6.0",
      "org.slf4j" % "slf4j-api" % "2.0.13"
    )
  )
