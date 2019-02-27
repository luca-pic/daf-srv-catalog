import Versions._
import Environment._
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

organization in ThisBuild := "it.gov.daf"
name := "daf-srv-catalog"


Seq(gitStampSettings: _*)

scalaVersion in ThisBuild := "2.11.12"
//scalaVersion in ThisBuild := "2.12.8"

lazy val root = (project in file(".")).enablePlugins(PlayScala, DockerPlugin)


libraryDependencies ++= Seq(
  ws,
  "it.gov.daf" %% "common" % Versions.dafCommonVersion,
  "org.mongodb" %% "casbah" % "3.1.1",
  //elastic
  "com.sksamuel.elastic4s" %% "elastic4s-http" % "5.6.4",
  "org.elasticsearch.client" % "elasticsearch-rest-client" % "5.6.2",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % Versions.scalaTest % "test",
  "org.mockito" % "mockito-scala_2.11" % "1.1.4"
//"io.swagger" %% "swagger-play2" % "1.5.1", already on common
  //"org.apache.spark" %% "spark-core" % "2.2.0",
  //"org.apache.spark" %% "spark-sql" % "2.2.0"
)

lazy val circe = "io.circe"

val circeDependencies = Seq(
  "circe-core",
  "circe-generic-extras",
  "circe-parser"
) map(circe %% _ % circeVersion)

libraryDependencies ++= circeDependencies
libraryDependencies += "play-circe" %% "play-circe" % "2.5-0.8.0"

resolvers ++= Seq(
  Resolver.mavenLocal,
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
  //  "jeffmay" at "https://dl.bintray.com/jeffmay/maven",
  //  Resolver.url("sbt-plugins", url("http://dl.bintray.com/gruggiero/sbt-plugins"))(Resolver.ivyStylePatterns),
  //"cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  //"lightshed-maven" at "http://dl.bintray.com/content/lightshed/maven",
  "daf repo" at s"$nexusUrl/maven-public/",
  "Bintary JCenter" at "http://jcenter.bintray.com"

)



// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
routesGenerator := InjectedRoutesGenerator


dockerBaseImage := "anapsix/alpine-java:8_jdk_unlimited"
dockerCommands := dockerCommands.value.flatMap {
  case cmd@Cmd("FROM", _) => List(cmd,
    Cmd("RUN", "apk update && apk add bash krb5-libs krb5"),
    Cmd("RUN", "ln -sf /etc/krb5.conf /opt/jdk/jre/lib/security/krb5.conf")
  )
  case other => List(other)
}

dockerExposedPorts := Seq(9000)

dockerEntrypoint := {Seq(s"bin/${name.value}", "-Dconfig.file=conf/production.conf")}

dockerRepository := Option(nexus)


publishTo in ThisBuild := {

  if (isSnapshot.value)
    Some("snapshots" at nexusUrl + "maven-snapshots/")
  else
    Some("releases"  at nexusUrl + "maven-releases/")
}

credentials += Credentials { Path.userHome / ".ivy2" / ".credentials" }
