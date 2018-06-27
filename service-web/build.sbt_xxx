name := "service-web"

version := "1.0"

lazy val `service-web` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(jdbc, cache, ws, specs2 % Test)

libraryDependencies ++= Seq("org.scalatest" % "scalatest_2.11" % "3.0.5")
libraryDependencies ++= Seq("log4j" % "log4j" % "1.2.7")
//libraryDependencies ++= Seq("org.slf4j" % "slf4j-log4j12" % "1.7.16")
unmanagedResourceDirectories in Test <+= baseDirectory(_ / "target/web/public/test")

      