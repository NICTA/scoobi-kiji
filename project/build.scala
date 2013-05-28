/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt._
import complete.DefaultParsers._
import Keys._
import com.typesafe.sbt._
import pgp.PgpKeys._
import sbt.Configuration
import SbtGit._
import GitKeys._
import sbtrelease._
import ReleasePlugin._
import ReleaseKeys._
import ReleaseStateTransformations._
import Utilities._
import Defaults._

object build extends Build {
  type Settings = Project.Setting[_]

  lazy val scoobiKiji = Project(
    id = "scoobi-kiji",
    base = file("."),
    settings = Defaults.defaultSettings ++
               scoobiSettings           ++
               dependenciesSettings     ++
               compilationSettings      ++
               testingSettings          ++
               publicationSettings      ++
               releaseSettings
  )

  lazy val scoobiKijiVersion = SettingKey[String]("scoobi-kiji-version", "defines the current Scoobi-Kiji version")
  lazy val scoobiSettings: Seq[Settings] = Seq(
    name := "scoobi-kiji",
    organization := "com.nicta",
    scoobiKijiVersion in GlobalScope <<= version,
    scalaVersion := "2.10.1")

  lazy val dependenciesSettings: Seq[Settings] = Seq(
    libraryDependencies ++= Seq(
      "com.nicta"              %% "scoobi"          % "0.7.0-RC1-cdh4",
      "org.kiji.schema"        %  "kiji-schema"     % "1.0.3",
      "org.kiji.mapreduce"     %  "kiji-mapreduce"  % "1.0.0-rc62"       % "provided",
      "org.apache.hbase"       %  "hbase"           % "0.94.3",
      "cglib"                  %  "cglib-nodep"     % "2.2.2"            ,
      "org.easymock"           %  "easymock"        % "3.1"              ,
      "org.kiji.schema"        %  "kiji-schema"     % "1.0.3"            classifier "tests",
      "org.kiji.testing"       %  "fake-hbase_2.10" % "0.0.5"            ,
      "org.specs2"             %% "specs2"          % "2.0-RC2-SNAPSHOT",
      "org.mockito"            %  "mockito-all"     % "1.9.0"),
    resolvers ++= Seq(
      "wibidata" at "https://repo.wibidata.com/artifactory/kiji",
      "sonatype-releases" at "http://oss.sonatype.org/content/repositories/releases",
      "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots")
    )

  lazy val compilationSettings: Seq[Settings] = Seq(
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:_"),
    scalacOptions in Test ++= Seq("-Yrangepos")
  )

  lazy val testingSettings: Seq[Settings] = Seq(
    // run each test in its own jvm
    fork in Test := true,
    javaOptions in Test ++= Seq("-Xmx1g")
  )

  lazy val publicationSettings: Seq[Settings] = Seq(
    publishTo <<= version { v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
      else                             Some("staging" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { x => false },
    pomExtra := (
      <url>http://nicta.github.io/scoobi-kiji</url>
      <licenses>
        <license>
          <name>Apache 2.0</name>
          <url>http://www.opensource.org/licenses/Apache-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>http://github.com/NICTA/scoobi</url>
        <connection>scm:http:http://NICTA@github.com/NICTA/scoobi-kiji.git</connection>
      </scm>
      <developers>
        <developer>
          <id>blever</id>
          <name>Ben Lever</name>
          <url>http://github.com/blever</url>
        </developer>
        <developer>
          <id>etorreborre</id>
          <name>Eric Torreborre</name>
          <url>http://etorreborre.blogspot.com/</url>
        </developer>
      </developers>
    ),
    credentials := Seq(Credentials(Path.userHome / ".sbt" / "scoobi.credentials"))
  )

  /**
   * RELEASE PROCESS
   */
  lazy val releaseSettings =
    ReleasePlugin.releaseSettings ++ Seq(
    tagName <<= (version in ThisBuild) map (v => "SCOOBI-KIJI" + v),
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      updateLicences,
      publishSignedArtifacts,
      tagRelease,
      setNextVersion,
      commitNextVersion,
      pushChanges
    ),
    releaseSnapshotProcess := Seq[ReleaseStep](
      publishSignedArtifacts),
    commands += releaseSnapshotCommand
  )

  lazy val releaseSnapshotProcess = SettingKey[Seq[ReleaseStep]]("release-snapshot-process")
  private lazy val releaseSnapshotCommandKey = "release-snapshot"
  private val WithDefaults = "with-defaults"
  private val SkipTests = "skip-tests"
  private val releaseSnapshotParser = (Space ~> WithDefaults | Space ~> SkipTests).*

  val releaseSnapshotCommand: Command = Command(releaseSnapshotCommandKey)(_ => releaseSnapshotParser) { (st, args) =>
    val extracted = Project.extract(st)
    val releaseParts = extracted.get(releaseSnapshotProcess)

    val startState = st
      .put(useDefaults, args.contains(WithDefaults))
      .put(skipTests, args.contains(SkipTests))

    val initialChecks = releaseParts.map(_.check)
    val process = releaseParts.map(_.action)

    initialChecks.foreach(_(startState))
    Function.chain(process)(startState)
  }

  lazy val updateLicences = ReleaseStep { st =>
    st.log.info("Updating the license headers")
    "mvn license:format" !! st.log
    commitCurrent("added license headers where missing")(st)
  }

  def testTaskDefinition(task: TaskKey[Tests.Output], options: Seq[TestOption]) =
    Seq(testTask(task))                          ++
    inScope(GlobalScope)(defaultTestTasks(task)) ++
    inConfig(Test)(testTaskOptions(task))        ++
    (testOptions in (Test, task) ++= options)

  def testTask(task: TaskKey[Tests.Output]) =
    task <<= (streams in Test, loadedTestFrameworks in Test, testLoader in Test,
      testGrouping in Test in test, testExecution in Test in task,
      fullClasspath in Test in test, javaHome in test) flatMap Defaults.allTestGroupsTask

  /**
   * PUBLICATION
   */
  lazy val publishSignedArtifacts = executeStepTask(publishSigned, "Publishing signed artifacts")

  /**
   * UTILITIES
   */
  private def executeStepTask(task: TaskKey[_], info: String) = ReleaseStep { st: State =>
    executeTask(task, info)(st)
  }

  private def executeTask(task: TaskKey[_], info: String) = (st: State) => {
    st.log.info(info)
    val extracted = Project.extract(st)
    val ref: ProjectRef = extracted.get(thisProjectRef)
    extracted.runTask(task in ref, st)._1
  }

  private def executeStepTask(task: TaskKey[_], info: String, configuration: Configuration) = ReleaseStep { st: State =>
    executeTask(task, info, configuration)(st)
  }

  private def executeTask(task: TaskKey[_], info: String, configuration: Configuration) = (st: State) => {
    st.log.info(info)
    val extracted = Project.extract(st)
    val ref: ProjectRef = extracted.get(thisProjectRef)
    extracted.runTask(task in configuration in ref, st)._1
  }

  private def commitCurrent(commitMessage: String): State => State = { st: State =>
    vcs(st).add(".") !! st.log
    val status = (vcs(st).status !!) trim

    if (status.nonEmpty) {
      vcs(st).commit(commitMessage) ! st.log
      st
    } else st
  }

  private def pushCurrent: State => State = { st: State =>
    vcs(st).pushChanges !! st.log
    st
  }

  private def vcs(st: State): Vcs = {
    st.extract.get(versionControlSystem).getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

}
