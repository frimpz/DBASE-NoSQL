name := "DBase"

version := "0.1"

scalaVersion := "2.11.12"

//logLevel := Level.Debug
showSuccess := false

libraryDependencies ++= Seq("org.apache.spark" %% "spark-mllib" % "2.4.2",
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2",
  "org.apache.spark" %% "spark-mllib" % "2.4.2",
  "co.theasi" %% "plotly" % "0.2.0",
  "org.vegas-viz" %% "vegas" % "0.3.11"
)

