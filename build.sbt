ThisBuild / scalaVersion := "2.13.13"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.say5"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "sparkscale",
    libraryDependencies ++= Seq(
      "org.apache.spark"   %% "spark-core"  % sparkVersion % "provided",
      "org.apache.spark"   %% "spark-sql"   % sparkVersion % "provided",
      "org.scalatest"      %% "scalatest"   % "3.2.18"     % Test,
    ),
    fork := true,
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
    ),
  )
