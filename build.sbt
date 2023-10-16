ThisBuild / scalaVersion := "3.3.0"

ThisBuild / organization := "com.clairvoyant.data.scalaxy"

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  System.getenv("GITHUB_USERNAME"),
  System.getenv("GITHUB_TOKEN")
)

// ----- RESOLVERS ----- //

ThisBuild / resolvers ++= Seq(
  "DataScalaxyTestUtil Repo" at "https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-test-util/"
)

// ----- PUBLISH TO GITHUB PACKAGES ----- //

ThisBuild / publishTo := Some(
  "Github Repo" at s"https://maven.pkg.github.com/teamclairvoyant/data-scalaxy-reader/"
)

// ----- SCALAFIX ----- //

ThisBuild / semanticdbEnabled := true
ThisBuild / scalafixOnCompile := true

// ----- WARTREMOVER ----- //

ThisBuild / wartremoverErrors ++= Warts.allBut(
  Wart.Any,
  Wart.DefaultArguments,
  Wart.Equals,
  Wart.FinalCaseClass,
  Wart.ImplicitParameter,
  Wart.LeakingSealed,
  Wart.Null,
  Wart.Overloading,
  Wart.Throw,
  Wart.TryPartial,
  Wart.ToString
)

// ----- TOOL VERSIONS ----- //

val dataScalaxyTestUtilVersion = "1.0.0"
val jsoupVersion = "1.16.1"
val scalaParserCombinatorsVersion = "2.3.0"
val sparkVersion = "3.4.1"
val sparkXMLVersion = "0.16.0"
val zioConfigVersion = "4.0.0-RC16"

// ----- TOOL DEPENDENCIES ----- //

val dataScalaxyTestUtilDependencies = Seq(
  "com.clairvoyant.data.scalaxy" %% "test-util" % dataScalaxyTestUtilVersion % Test
)

val jsoupDependencies = Seq(
  "org.jsoup" % "jsoup" % jsoupVersion
)

val scalaParserCombinatorsDependencies = Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % scalaParserCombinatorsVersion
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
  .map(_ excludeAll ("org.scala-lang.modules", "scala-xml"))
  .map(_.cross(CrossVersion.for3Use2_13))

val sparkXMLDependencies = Seq(
  "com.databricks" %% "spark-xml" % sparkXMLVersion
).map(_.cross(CrossVersion.for3Use2_13))

val zioConfigDependencies = Seq(
  "dev.zio" %% "zio-config-magnolia" % zioConfigVersion
).map(_ excludeAll ("org.scala-lang.modules", "scala-collection-compat"))

// ----- MODULE DEPENDENCIES ----- //

val textDependencies =
  dataScalaxyTestUtilDependencies ++
    jsoupDependencies ++
    sparkDependencies ++
    sparkXMLDependencies ++
    zioConfigDependencies

// ----- PROJECTS ----- //

lazy val `data-scalaxy-reader` = (project in file("."))
  .settings(
    publish / skip := true,
    publishLocal / skip := true
  )
  .aggregate(`reader-text`)

lazy val `reader-text` = (project in file("text"))
  .settings(
    version := "1.1.0",
    libraryDependencies ++= textDependencies
  )
