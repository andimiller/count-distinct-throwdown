import sbt.librarymanagement._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .enablePlugins(JavaAgent)
  .settings(
    name := "count-distinct-throwdown",
    javaAgents += "com.github.jbellis" % "jamm" % "0.3.3" % "runtime",
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),
    libraryDependencies ++= List(
      // shit we want to use for test harnesses
      "org.typelevel" %% "cats-effect-std" % "3.4.8",
      "org.typelevel" %% "cats-effect" % "3.4.8",
      "co.fs2" %% "fs2-core" % "3.6.1",
      "co.fs2" %% "fs2-io" % "3.6.1",
      "org.typelevel" %% "squants" % "1.8.3",
      "org.gnieh" %% "fs2-data-csv" % "1.6.1",
      "org.gnieh" %% "fs2-data-csv-generic" % "1.6.1",
      "org.atnos" %% "origami-core" % "6.1.1",
      // something to estimate sizes
      "com.github.jbellis" % "jamm" % "0.3.3",
      // shit we're about to test
      "com.dynatrace.hash4j" % "hash4j" % "0.8.0",
      "com.addthis" % "stream-lib" % "3.0.0",
      "org.apache.datasketches" % "datasketches-java" % "3.3.0"
    )
  )
