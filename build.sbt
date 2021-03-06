name := "Async mysql driver"

version := "1.0"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0-M1"

libraryDependencies += "org.specs2" %% "specs2" % "1.7" % "test"

libraryDependencies += "junit" % "junit" % "4.7"
