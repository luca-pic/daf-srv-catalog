package it.gov.daf.config

import it.gov.daf.common.config.Read

case class NifiConfig(host: String, port: Int)

object NifiConfig {

  private def readConfig = Read.config { "nifi" }.!

  def readValues = for {
    host         <- Read.string{ "host" }.!
    port        <- Read.int{ "port" }.!
  } yield NifiConfig(
    host = host,
    port = port
  )

  def reader = readConfig ~> readValues

}
