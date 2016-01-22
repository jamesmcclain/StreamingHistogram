lazy val commonSettings = Seq(
  organization := "com.daystrom-data-concepts",
  version := "33",
  scalaVersion := "2.11.7"
)

lazy val histogram = (project in file("histogram")).
  settings(commonSettings: _*).
  settings(
    name := "histogram"
  )

lazy val main = (project in file("main")).
  settings(commonSettings: _*).
  settings(
    name := "main"
  )
