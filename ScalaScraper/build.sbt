name := "History of Globalization"

version := "0.1"

scalaVersion := "2.12.4"

// scala-xml
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.0"

// circe (json)
val circeVersion = "0.9.1"
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
        
// macro paradise plugin for circe JsonCodec
resolvers += Resolver.sonatypeRepo("releases")
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

// scala-csv
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"