import com.github.retronym.SbtOneJar._
import scoverage.ScoverageKeys._
import sbtfilter.Plugin._
import sbt.Keys._
import sbt._

val projectScalaVersion = "2.12.1"
val projectVersion = "0.1.0-SNAPSHOT"

val vBijection = "0.9.5"
val vCats = "0.9.0"
val vCirce = "0.7.0"
val vLogback = "1.2.2"
val vScalaTest = "3.0.1"
val vSlf4j = "1.7.25"

val bijection = Seq(
  "com.twitter" %% "bijection-core" % vBijection
)

val cats = Seq(
  "org.typelevel" %% "cats-core" % vCats
)

val circe = Seq(
  "io.circe" %% "circe-core" % vCirce,
  "io.circe" %% "circe-generic" % vCirce,
  "io.circe" %% "circe-jawn" % vCirce
)

val logback = Seq(
  "ch.qos.logback" % "logback-classic"   % vLogback,
  "org.slf4j"      %  "jul-to-slf4j"     % vSlf4j,
  "org.slf4j"      %  "jcl-over-slf4j"   % vSlf4j,
  "org.slf4j"      %  "log4j-over-slf4j" % vSlf4j
)

val scalaTest = Seq(
  "org.scalatest" %% "scalatest" % vScalaTest % "test"
)

val slf4j = Seq(
  "org.slf4j" %  "slf4j-api" % vSlf4j
)

val extraSettings = Defaults.coreDefaultSettings

val sharedSettings = extraSettings ++ filterSettings ++ Seq(
  organization := "io.github.benwhitehead.envelope",
  scalaVersion := projectScalaVersion,
  version := projectVersion,

  exportJars := true,

  libraryDependencies ++=
    scalaTest
      ++ slf4j,

  javacOptions in Compile ++= Seq(
    "-source", "1.8",
    "-target", "1.8",
    "-Xlint:unchecked",
    "-Xlint:deprecation"
  ),

  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding", "UTF-8",
    "-explaintypes",
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-target:jvm-1.8",
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xfuture",
    "-Xlint", // Enable recommended additional warnings.
    "-Yresolve-term-conflict:package",
    "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Ywarn-dead-code",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"
  ),

  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Ywarn-unused-import")),

  scalacOptions in (Test, console) ~= (_ filterNot (_ == "-Ywarn-unused-import")),

  // Publishing options:
  publishMavenStyle := true,

  pomIncludeRepository := { x => false },

  publishArtifact in Test := false,

  parallelExecution in ThisBuild := false,

  parallelExecution in Test := false,

  fork := false,

  cancelable in Global := true

)

lazy val root = Project("envelope", file("."))
  .settings(sharedSettings)
  .settings(
    libraryDependencies ++=
      bijection
      ++ cats
      ++ circe
  )
