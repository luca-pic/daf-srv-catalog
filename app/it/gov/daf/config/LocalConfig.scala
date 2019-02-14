package it.gov.daf.config

import it.gov.daf.common.config.Read

case class LocalConfig(host: String, port: Int)

object LocalConfig {

  private def readConfig = Read.config { "local" }.!

  def readValues = for {
    host         <- Read.string{ "host" }.!
    port        <- Read.int{ "port" }.!
  } yield LocalConfig(
    host = host,
    port = port
  )

  def reader = readConfig ~> readValues
}