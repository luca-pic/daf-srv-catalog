package it.gov.daf.config

import it.gov.daf.common.config.Read

case class SerivicesConfig(datipubbliciUrl: String, securityUrl: String)

object SerivicesConfig {

  private def readConfig = Read.config { "services" }.!

  def readValues = for {
    datipubbliciUrl    <- Read.string{ "datipubbliciUrl" }.!
    securityUrl        <- Read.string{ "securityUrl" }.!
  } yield SerivicesConfig(
    datipubbliciUrl = datipubbliciUrl,
    securityUrl = securityUrl
  )

  def reader = readConfig ~> readValues

}
