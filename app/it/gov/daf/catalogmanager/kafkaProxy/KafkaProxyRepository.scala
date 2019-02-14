package it.gov.daf.catalogmanager.kafkaProxy

import javax.inject.Inject
import play.api.Logger
import play.api.libs.json.Json
import it.gov.daf.model.{Error, Success, MetaCatalog}
import play.api.libs.ws.{WSClient, WSResponse}
import it.gov.daf.config.KafkaProxyConfig
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.Future
import scala.util.{Either, Left, Right}
import scala.concurrent.ExecutionContext.Implicits.global

class KafkaProxyRepository @Inject()(implicit ws: WSClient, val kafkaProxyConfig: KafkaProxyConfig){


  private val kafkaProxyHost: String = kafkaProxyConfig.host
  private val kafkaProxyPort: Int = kafkaProxyConfig.port
  private val KAFKA_PROXY_URL = s"$kafkaProxyHost:$kafkaProxyPort"
  private val logger = Logger(this.getClass.getName)


  def sendMessageKafkaProxy(user: String, metaCatalog: MetaCatalog, token: String): Future[Either[Error, Success]] = {
    logger.debug(s"kafka proxy $KAFKA_PROXY_URL")
    val jsonMetacatol = metaCatalog.asJson
    val jsonUser = s""""user":"$user""""
    val jsonToken = s""""token":"$token""""
    val jsonBody = Json.parse(
      s"""
         |{
         |"records":[{"value":{$jsonUser,$jsonToken,"payload":$jsonMetacatol}}]
         | }
              """.stripMargin)

    val responseWs: Future[WSResponse] = ws.url(KAFKA_PROXY_URL + "/topics/creationfeed")
      .withHeaders(("Content-Type", "application/vnd.kafka.v2+json"))
      .post(jsonBody)

    responseWs.map{ res =>
      res.status match {
        case 200 => logger.debug(s"message sent to kakfa proxy for user $user"); Right(Success("sended", None))
        case _   => logger.debug(s"error in sending message to kafka proxy for user $user"); Left(Error(Some(500), s"error in sending message to kafka proxy for user $user", None))
      }
    }
  }

  def sendGenericMessageToKafka(group: Option[String], user: Option[String], topic: String, notificationType: String, title: String, description: String, link: Option[String], token: String) ={
    logger.debug(s"kafka proxy $KAFKA_PROXY_URL, topic $topic")

    val receiver = user match {
      case Some(x) => s""""user":"$x""""
      case None    => s""""group":"${group.get}""""
    }

    val message = s"""{
                     |"records":[{"value":{$receiver, "token":"$token","notificationtype": "$notificationType", "info":{
                     |"title":"$title","description":"$description","link":"${link.getOrElse("")}"}}}]}""".stripMargin

    val jsonBody = Json.parse(message)

    logger.debug(s"try to send $jsonBody to $receiver")

    val responseWs = ws.url(KAFKA_PROXY_URL + s"/topics/$topic")
      .withHeaders(("Content-Type", "application/vnd.kafka.v2+json"))
      .post(jsonBody)

    responseWs.map{ res =>
      if( res.status == 200 ) {
        logger.debug(s"message sent to kakfa proxy for ${user.getOrElse(group.get)} in topic $topic")
        Right(Success(s"${user.getOrElse(group.get)} sent to kafka in topic $topic", None))
      }
      else {
        logger.debug(s"error in sending message to kafka proxy for ${user.getOrElse(group.get)} in topic $topic")
        Left(Error(Some(500), s"error in sending message to kafka proxy for user ${user.getOrElse(group.get)}", None))
      }
    }
  }

}
