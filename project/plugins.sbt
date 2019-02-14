//resolvers +=  Resolver.url("sbt-plugins", url("http://dl.bintray.com/gruggiero/sbt-plugins"))(Resolver.ivyStylePatterns)

resolvers += "scalaz-bintray"   at "http://dl.bintray.com/scalaz/releases"

//resolvers += Resolver.url("hmrc-sbt-plugin-releases", url("https://dl.bintray.com/hmrc/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("uk.gov.hmrc" % "sbt-git-stamp" % "5.3.0")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.5.14")

//addSbtPlugin("org.lyranthe.sbt" % "partial-unification" % "1.1.0")

//addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.0-M9")

//addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.3.0")

//addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

//addSbtPlugin("de.heikoseeberger" % "sbt-header" % "2.0.0")
