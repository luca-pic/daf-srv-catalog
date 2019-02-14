package it.gov.daf.config

import it.gov.daf.common.config.Read

case class ElasticsearchConfig(host: String, port: Int)

object ElasticsearchConfig {

  private def readConfig = Read.config { "elasticsearch" }.!

  def readValues = for {
    host         <- Read.string{ "host" }.!
    port        <- Read.int{ "port" }.!
  } yield ElasticsearchConfig(
    host = host,
    port = port
  )

  def reader = readConfig ~> readValues

}
