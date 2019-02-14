package it.gov.daf.config

import it.gov.daf.common.config.Read

case class KyloConfig(url: String, user: String, userPwd: String, sftHostName: String)

object KyloConfig {

  private def readConfig = Read.config { "kylo" }.!

  def readValues = for {
    url         <- Read.string{ "url" }.!
    user        <- Read.string{ "user" }.!
    userpwd     <- Read.string{ "userpwd" }.!
    sftHostName <- Read.string{ "sftpHostname" }.!
  } yield KyloConfig(
    url = url,
    user = user,
    userPwd = userpwd,
    sftHostName = sftHostName
  )

  def reader = readConfig ~> readValues

}
