package it.gov.daf.catalogmanager.nifi

import com.google.inject.{Inject, Singleton}
import catalog_manager.yaml.{Error, Success}
import cats.data.EitherT
import play.api.inject.ConfigurationProvider
import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.Logger
import cats.implicits._
import it.gov.daf.catalogmanager.utilities.{ErrorWrapper, NifiRevisionObject, ProcessHandler}

import scala.concurrent.Future
import scala.util
import scala.util.{Failure, Try}


//case class ErrorWrapper(error: Error, steps:Int)
//case class NifiRevisionObject(clientId: String, version: Int)

@Singleton
class Nifi @Inject()(ws :WSClient, config: ConfigurationProvider) {

  import scala.concurrent.ExecutionContext.Implicits._

  private val NifiURL = config.get.getString("nifi.url").get

  private val RecoveryAreaProcessorName = "Get File from Recovery Area"
  private val StopStateProcessorString = "STOPPED"
  private val RunStateProcessorString = "RUNNING"
  private val DirectoryPropertyName = "Directory"

  private val logger = Logger(this.getClass.getName)

  def startDerivedProcessor(orgName: String, feedName: String): Future[Either[Error, Success]] = {

    logger.info(s"startDerivedProcessor, org: $orgName, feed: $feedName")

    getProcessors.flatMap { resp =>
      val res: EitherT[Future, ErrorWrapper, Success] = for {
        id          <- ProcessHandler.step(extractIdFromJsonByFeedNameAndOrgName(resp, orgName, feedName))
        revisionObj <- ProcessHandler.step(getProcessorRevision(id))
        _           <- ProcessHandler.step(setStateProcessor(id, revisionObj.clientId, revisionObj.version, StopStateProcessorString))
        _           <- ProcessHandler.step(setStateProcessor(id, revisionObj.clientId, revisionObj.version + 1, RunStateProcessorString))
        res         <- ProcessHandler.step(setStateProcessor(id, revisionObj.clientId, revisionObj.version + 2, StopStateProcessorString))

      } yield res

      res.value.map {
        case Right(success) =>
          logger.debug(s"processor of feed $feedName started")
          Right(success)
        case Left(error) =>
          logger.debug(s"error in start feed $feedName: $error")
          Left(error.error)
      }
    }
  }

  private def parseJsValueToString(jsValue: JsValue): String = jsValue.toString().replaceAll("\\\"", "")

  private def getProcessors: Future[WSResponse] = {
    ws.url(s"$NifiURL/nifi-api/flow/process-groups/root/status?recursive=true")
      .withHeaders("Content-type" -> "application/json")
      .get()
  }

  private def extractIdFromJsonByFeedNameAndOrgName(responseNifi: WSResponse, orgName: String, feedName: String): Future[Either[Error, String]] = {
    logger.debug(s"nifi reponse for feed $feedName is ${responseNifi.statusText}")

    val tryGetId: Try[String] = Try {
      val orgJson: JsValue = (responseNifi.json \ "processGroupStatus" \ "aggregateSnapshot" \ "processGroupStatusSnapshots" \\ "processGroupStatusSnapshot")
        .filter(jsValue =>
          (jsValue \ "name").getOrElse(JsString("")).as[String].equals(orgName)
        ).head
      val feedJson: JsValue = (orgJson \ "processGroupStatusSnapshots" \\ "processGroupStatusSnapshot")
        .filter(jsValue =>
          (jsValue \ "name").getOrElse(JsString("")).as[String].equals(feedName)
        ).head
      val id: JsValue = (feedJson \ "processorStatusSnapshots" \\ "processorStatusSnapshot")
        .filter(jsValue => (jsValue \ "name").getOrElse(JsString("")).as[String].equals("Run Feed")).map(x => x \ "id")
        .head.get
      id.as[String]
    }

    tryGetId match {
      case Failure(exception) =>
        logger.debug(s"error in extract id: ${exception.getMessage}")
        Future.successful(Left(Error(s"error in extract id: ${exception.getMessage}", Some(500), None)))
      case util.Success(id) =>
        logger.debug(s"id for $feedName is $id")
        Future.successful(Right(id))
    }
  }

  private def getRevision(id: String): Future[(String, Int)] = {
    import java.util.UUID.randomUUID
    ws.url(s"$NifiURL/nifi-api/processors/$id").get()
      .map { resp =>
        val json: JsLookupResult = resp.json \ "revision"
        logger.debug(s"$id: revision{ $json }")
        (
          parseJsValueToString((json \ "clientId").getOrElse(JsString(randomUUID().toString))),
          parseJsValueToString((json \ "version").get).toInt
        )
      }
  }

  private def setStateProcessor(id: String, clientId: String, version: Int, state: String): Future[Either[Error, Success]] = {
    val json: JsValue = Json.parse(
      s"""
        |{
        |  "revision": {
        |    "clientId": "$clientId",
        |    "version": $version
        |  },
        |  "component": {
        |    "id": "$id",
        |    "state": "${state.toUpperCase}"
        |  }
        |}
      """.stripMargin
    )
    ws.url(s"$NifiURL/nifi-api/processors/$id").withHeaders("Content-type" -> "application/json")
      .put(json)
      .map{ res =>
        Logger.debug(s"nifi response status $state: $res")
        res.status match {
          case 200 => logger.debug(s"processor $id updatate to state $state");Right(Success(res.body, None))
          case _   => logger.debug(s"processor $id not updatate to state $state"); Left(Error(res.body, Some(res.status), None))
        }
      }
  }

  private def getProcessorRevision(id: String): Future[Either[Error, NifiRevisionObject]] = {
    import java.util.UUID.randomUUID
    logger.debug(s"call nifi to get processor's revision object: $NifiURL/nifi-api/processors/$id")
    ws.url(s"$NifiURL/nifi-api/processors/$id").get()
      .map{resp =>
        logger.debug(s"nifi response: ${resp.status} ${resp.body}")
        val revisionObj = Try{
          NifiRevisionObject(
            (resp.json \ "revision" \ "clientId").getOrElse(JsString(randomUUID().toString)).as[String],
            (resp.json \ "revision" \ "version").as[Int]
          )
        }
        revisionObj match {
          case Failure(exception)  =>
            logger.debug(s"error in get revision object: ${exception.getMessage}, ${exception.getStackTrace}")
            Left(Error(s"error in get revision object: ${exception.getMessage}", Some(500), None))
          case util.Success(value) =>
            logger.debug(s"revision object fot processor $id: $value")
            Right(value)
        }
      }
  }

  private def updateDirectoryProcRecoveryArea(idProc: String, nameProperty: String, valueProperty: String, nifiRevisionObject: NifiRevisionObject): Future[Either[Error, Success]] = {
    val jsonBody = Json.parse(
      s"""
         |{
         |  "component": {
         |    "id": "$idProc",
         |    "config": {
         |      "properties": {
         |        "$nameProperty": "$valueProperty"
         |      }
         |    }
         |  },
         |  "revision": {
         |    "clientId": "${nifiRevisionObject.clientId}",
         |    "version": ${nifiRevisionObject.version}
         |  }
         |}
       """.stripMargin
    )

    ws.url(s"$NifiURL/nifi-api/processors/$idProc").withHeaders("Content-type" -> "application/json")
        .put(jsonBody)
        .map{ res =>
          logger.debug(s"nifi response update directory recovery area ${res.status}: ${res.body}")
          res.status match {
            case 200 => logger.debug(s"update properties $nameProperty to $valueProperty");Right(Success(res.body, None))
            case _   => logger.debug(s"error in update properties $nameProperty to $valueProperty"); Left(Error(res.body, Some(res.status), None))
          }
        }
  }

  def startRecoveryAreaProcessor(componentId: String, pathRecoveryArea: String): Future[Either[Error, Success]]= {

    logger.info(s"startRecoveryAreaProcessor: $componentId")

//    val resp: EitherT[Future, ErrorWrapper, Success] = for{
//      a                  <- ProcessHandler.step2( getProcessorIdByComponentId(componentId, RecoveryAreaProcessorName) )
//      (idProcRecoveryArea: String, step0: Int) = (a._1, a._2)
//      b                  <- ProcessHandler.step2( step0, getProcessorRevision(idProcRecoveryArea) )
//      (nifiRevisionObject: NifiRevisionObject, step1: Int) = (b._1, b._2)
//      c                  <- ProcessHandler.step2( step1, updateDirectoryProcRecoveryArea(idProcRecoveryArea, DirectoryPropertyName, pathRecoveryArea, nifiRevisionObject) )
//      (_, step2: Int) = (c._1, c._2)
//      d                  <- ProcessHandler.step2( step2, setStateProcessor(idProcRecoveryArea, nifiRevisionObject.clientId, nifiRevisionObject.version, StopStateProcessorString) )
//      (_, step3: Int) = (d._1, d._2)
//      e                  <- ProcessHandler.step2( step3, setStateProcessor(idProcRecoveryArea, nifiRevisionObject.clientId, nifiRevisionObject.version, RunStateProcessorString) )
//      (response: Success, _) = (e._1, e._2)
//    } yield response

    val resp: EitherT[Future, ErrorWrapper, Success] = for{
      a                  <- ProcessHandler.step2( getProcessorIdByComponentId(componentId, RecoveryAreaProcessorName) )
      (idProcRecoveryArea: String, step0: Int) = (a._1, a._2)
      b                  <- ProcessHandler.step2( step0, getProcessorRevision(idProcRecoveryArea) )
      (nifiRevisionObject: NifiRevisionObject, step1: Int) = (b._1, b._2)
      c                  <- ProcessHandler.step2( step1, configureRecoveyAreaProcessor(idProcRecoveryArea, pathRecoveryArea, nifiRevisionObject) )
      (response, _) = (c._1, c._2)
    } yield response

    resp.value.map{
      case Right(value) =>
        logger.debug(s"response startRecoveryAreaProcessor: $value")
        Right(value)
      case Left(error) =>
        logger.debug(s"error in startRecoveryAreaProcessor: $error")
        Left(error.error)
    }
  }

  private def configureRecoveyAreaProcessor(idProcRecoveryArea: String, pathRecoveryArea: String, nifiRevisionObject: NifiRevisionObject): Future[Either[Error, Success]] = {
    updateDirectoryProcRecoveryArea(idProcRecoveryArea, DirectoryPropertyName, pathRecoveryArea, nifiRevisionObject) flatMap{
      case Right(_)    => runStopAndRunProcessor(idProcRecoveryArea, nifiRevisionObject.clientId, nifiRevisionObject.version)
      case Left(error) => Future.successful(Left(error))
    }
  }

  private def runStopAndRunProcessor(idProc: String, clientId: String, version: Int): Future[Either[Error, Success]] = {
    setStateProcessor(idProc, clientId, version, StopStateProcessorString) flatMap{
      case Right(_) =>
        setStateProcessor(idProc, clientId, version, RunStateProcessorString)
      case Left(error) => Future.successful(Left(error))
    }
  }

  private def getProcessorIdByComponentId(componentId: String, processorName: String): Future[Either[Error, String]] = {
    logger.debug(s"call nifi: $NifiURL/nifi-api/process-groups/$componentId/processors")
    ws.url(s"$NifiURL/nifi-api/process-groups/$componentId/processors")
      .withHeaders("Content-type" -> "application/json")
      .get()
      .map{ res =>
        logger.debug(s"response: ${res.status}, ${res.body}")
        if(res.status != 200 || res.body.isEmpty) {
          logger.debug(s"error in get processors, nifi response ${res.status} ${res.body}")
          Left(Error("errror in get processors", Some(500), None))
        }
        else{
          val optionJsonProcessor: Option[JsValue] = (res.json \ "processors").as[JsArray].value
            .find(elem =>
              (elem \ "status" \ "name").as[String].equals(processorName))

          optionJsonProcessor match {
            case Some(json) =>
              json \ "status" \ "id" match {
                case JsDefined(value) =>
                  logger.debug(s"$processorName id: ${value.as[String]}")
                  Right(value.as[String])
                case error: JsUndefined =>
                  logger.debug(s"error in extract id of processor $processorName: $error")
                  Left(Error(s"error in extract id of processor $processorName", Some(500), None))
              }
            case None =>
              logger.debug(s"error in find json of processor $processorName")
              Left(Error(s"error in find processor $processorName", Some(404), None))
          }
        }
      }
  }

}
