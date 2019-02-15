package it.gov.daf.config

import it.gov.daf.common.config.Read

case class ServicesConfig(datipubbliciUrl: String, securityUrl: String)

object ServicesConfig {

  private def readConfig = Read.config { "services" }.!

  def readValues = for {
    datipubbliciUrl    <- Read.string{ "datipubbliciUrl" }.!
    securityUrl        <- Read.string{ "securityUrl" }.!
  } yield ServicesConfig(
    datipubbliciUrl = datipubbliciUrl,
    securityUrl = securityUrl
  )

  def reader = readConfig ~> readValues

}
