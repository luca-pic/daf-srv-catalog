package it.gov.daf.config

import it.gov.daf.common.config.Read

case class KafkaProxyConfig(host: String, port: Int, dafAdminGroup: String)

object KafkaProxyConfig {

  private def readConfig = Read.config { "kafkaProxy" }.!

  def readValues = for {
    host          <- Read.string{ "host" }.!
    port          <- Read.int{ "port" }.!
    dafAdminGroup <- Read.string{ "dafAdminGroup" }.!
  } yield KafkaProxyConfig(
    host = host,
    port = port,
    dafAdminGroup = dafAdminGroup
  )

  def reader = readConfig ~> readValues
}