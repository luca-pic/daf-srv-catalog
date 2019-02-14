package it.gov.daf.config

import it.gov.daf.common.config.Read

case class CkanGeoConfig(host: String, port: Int)

object CkanGeoConfig {

  private def readConfig = Read.config { "ckan-geo" }.!

  def readValues = for {
    host         <- Read.string{ "host" }.!
    port        <- Read.int{ "port" }.!
  } yield CkanGeoConfig(
    host = host,
    port = port
  )

  def reader = readConfig ~> readValues

}