package it.gov.daf.catalogmanager.kylo

import catalog_manager.yaml.{InputSrcSrv_pullOpt, MetaCatalog, SourceSftp}
import play.api.libs.json.{JsObject, JsResult, JsValue}
import catalog_manager.yaml.{Error, InputSrcSrv_pullOpt, MetaCatalog, Success}
import play.api.libs.json._
import play.api.libs.ws.{WSAuthScheme, WSClient, WSResponse}
import com.google.inject.{Inject, Singleton}
import play.api.Logger
import play.libs.Json

import scala.util.Try
//import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType
import play.api.inject.ConfigurationProvider

import scala.concurrent.Future

@Singleton
class Kylo @Inject()(ws :WSClient, config: ConfigurationProvider){

  val KYLOURL = config.get.getString("kylo.url").get
  val KYLOUSER = config.get.getString("kylo.user").getOrElse("dladmin")
  val KYLOPWD = config.get.getString("kylo.userpwd").getOrElse("XXXXXXXXXXX")
  val SFTPHOSTNAME = config.get.getString("sftp.hostname").getOrElse("XXXXXXXXXXX")

  val RecoveryAreaProcessorName = "Get File from Recovery Area"

  import scala.concurrent.ExecutionContext.Implicits._

  private def getFeedInfo(feedName: String, user: String): Future[Either[Error, String]] = {
    val futureResponseFeedInfo = ws.url(KYLOURL + "/api/v1/feedmgr/feeds/by-name/" + feedName)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)
      .get()

    val res = futureResponseFeedInfo.map{
      resp =>
        val json: JsValue = Try(resp.json).getOrElse(JsNull)
        val id: Option[JsValue] = Try{Some((json \ "feedId").get)}.getOrElse(None)
        id match {
          case Some(x) => Logger.logger.debug(s"$feedName: $x"); Right(x.toString().replace("\"", ""))
          case None    => Logger.logger.debug(s"id of $feedName not found"); Left(Error(s"feed $feedName not found", Some(404), None))
        }
    }
    res
  }

  def deleteFeed(feedName: String, user: String): Future[Either[Error, Success]] = {
    getFeedInfo(feedName, user).flatMap{
      case Right(idFeed) =>
        for{
          _ <- disableFeed(idFeed)
          res <- patchDelete(user, feedName, idFeed)
        } yield res
      case Left(left) => Future.successful(Right(Success(left.message, None)))
    }
  }

  private def disableFeed(feedId: String): Future[Either[Error, Success]] = {
    val disbleFeedResponse: Future[WSResponse] = ws.url(KYLOURL + "/api/v1/feedmgr/feeds/disable/" + feedId)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC).withHeaders("Content-Type" -> "application/json")
      .post("")

    val futureResponseDisableFeed = disbleFeedResponse
      .map { res =>
        if (res.status == 200) {
          Logger.logger.debug(s"$feedId disabled")
          Right(Success(s"$feedId disabled", None))
        }
        else {
          Logger.logger.debug(s"$feedId not disabled")
          Left(Error(s"$feedId not disabled", Some(res.status), None))
        }
      }

    futureResponseDisableFeed
  }

  private def patchDelete(user: String, feedName: String, feedId: String): Future[Either[Error, Success]] = {
    val firstDelete: Future[Either[Error, Success]] = delete(user, feedName, feedId)

    firstDelete flatMap {
      case Right(success) => Future.successful(Right(success))
      case Left(_) => Logger.debug("second call"); Thread.sleep(10000); delete(user, feedName, feedId)
    }

  }

  private def delete(user: String, feedName: String, feedId: String): Future[Either[Error, Success]] = {
    val futureResponseDelete: Future[WSResponse] = ws.url(KYLOURL + "/api/v1/feedmgr/feeds/" + feedId)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)
      .delete()

    futureResponseDelete.map{ res =>
      res.status match {
        case 204 => Logger.logger.debug(s"$user deleted $feedName");     Right(Success(s"$feedName deleted", None))
        case 404 => Logger.logger.debug(s"$feedName not found");         Right(Success(s"$feedName not found", None))
        case _   => Logger.logger.debug(s"$user not deleted $feedName"); Left(Error(s"kylo feed $feedName ${Try{(res.json \ "message").get.toString().replace("\"", "")}.getOrElse("generic error")}", Some(res.status), None))
      }
    }
  }

  private def templateIdByName(templateName :String): Future[Option[String]] = {
    ws.url(KYLOURL + "/api/v1/feedmgr/templates/registered")
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get()
      .map { resp =>
        val js: JsValue = resp.json
        val result = js.as[List[JsValue]]
          .filter(x => (x \ "templateName").as[String].equals(templateName))
        val res = result match {
          case Nil => None
          case h :: _  => Some((h \ "id").as[String])
        }
        Logger.logger.debug(s"templateId: $res")
        res
      }
  }


  private def templatesById(id :Option[String], fileType :String, feed :MetaCatalog): Future[(JsValue, List[JsObject])] = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })

      val template1 = templatesEditable(0).transform(KyloTrasformers.transformTemplates(KyloTrasformers.generateInputSftpPath(feed)))
      val template2 = templatesEditable(1).transform(KyloTrasformers.transformTemplates(".*" + fileType))
      //logger.debug(List(template1,template2).toString())
      (templates, List(template1.get, template2.get))
    }
  }

  private def webServiceTemplates(id :Option[String], fileType: String, feed :MetaCatalog): Future[(JsValue, List[JsObject])] = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    val testWs  = feed.operational.input_src.srv_pull.get.head.url
    val nameExp = feed.dcatapit.name + """_${now():format('yyyyMMddHHmmss')}"""
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable  = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
      val modifiedTemplates: List[JsObject] = templatesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "URL" => temp.transform(KyloTrasformers.transformTemplates(testWs)).get
          case "Filename" => temp.transform(KyloTrasformers.transformTemplates(nameExp)).get
          case "Username" => temp.transform(KyloTrasformers.transformTemplates("dsda")).get
          case "Password" => temp.transform(KyloTrasformers.transformTemplates("dasds")).get
        }
        transTemplate
      }

      (templates, modifiedTemplates)
    }
  }

  private def hdfsTemplate(id: Option[String], fileType: String, hdfsPath: String) = {

    val url = s"/api/v1/feedmgr/templates/registered/${id.getOrElse("")}?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)
      .get().map { resp =>
      Logger.logger.debug(s"Kylo resp: ${resp.body}")

      val template = resp.json

      val propertiesEditable = (template \ "properties").as[List[JsValue]]
        .filter(x => (x \ "userEditable").as[Boolean])
      val modifiedTemplate: List[JsObject] = propertiesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "File Filter Regex" => temp.transform(KyloTrasformers.transformTemplates(".*" + fileType)).get
          case "Directory" => temp.transform(KyloTrasformers.transformTemplates(hdfsPath)).get
        }
        transTemplate
      }

      (template, modifiedTemplate.filterNot{x => (x \ "processorName").getOrElse(JsString("")).as[String].equals(RecoveryAreaProcessorName)})
    }
  }

  private def sftpTemplates(id :Option[String], fileType: String, sftpPath: String): Future[(JsValue, List[JsObject])] = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val x = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
        .filterNot(x => (x \ "processorName").getOrElse(JsString("")).as[String].equals(RecoveryAreaProcessorName))
      val templatesEditable  = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
//      val modifiedTemplates: List[JsObject] = templatesEditable.map { temp =>
      val modifiedTemplates: List[JsObject] = x.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "Hostname" => temp.transform(KyloTrasformers.transformTemplates(SFTPHOSTNAME)).get
          case "Remote Path" => temp.transform(KyloTrasformers.transformTemplates(sftpPath)).get
          case "File Filter Regex" => temp.transform(KyloTrasformers.transformTemplates(".*" + fileType)).get
        }
        transTemplate
      }
      Logger.logger.debug(s"modifiedTemplates: $modifiedTemplates")
      Logger.logger.debug(s"templates: $templates")
      (templates, modifiedTemplates)
    }
  }

  private def transformationTemplate(id :Option[String], fileType: String, sftpPath: String): Future[(JsValue, List[JsObject])] = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable  = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
      val modifiedTemplates: List[JsObject] = templatesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "Hostname" => temp.transform(KyloTrasformers.transformTemplates(SFTPHOSTNAME)).get
          case "Remote Path" => temp.transform(KyloTrasformers.transformTemplates(sftpPath)).get
          case "File Filter Regex" => temp.transform(KyloTrasformers.transformTemplates(".*" + fileType)).get
        }
        transTemplate
      }

      (templates, modifiedTemplates)
    }
  }


  def datasetIngest(fileType :String, meta :MetaCatalog) :Future[(JsValue, List[JsObject])] = {
    for {
      idOpt <- templateIdByName("Dataset Ingest")
      templates <- templatesById(idOpt, fileType, meta)
    } yield templates
  }

  def wsIngest(fileType: String, meta: MetaCatalog): Future[(JsValue, List[JsObject])] = {
    for {
      idOpt <- templateIdByName("Webservice & AUTOREC Ingest")
//      idOpt <- templateIdByName("Webservice Ingest")
      templates <- webServiceTemplates(idOpt, fileType, meta)
    } yield templates
  }

  def sftpRemoteIngest(fileType: String, sftpPath: String): Future[(JsValue, List[JsObject])] = {
    for {
      idOpt <- templateIdByName("SFTP & AUTOREC Ingest")
//      idOpt <- templateIdByName("Sftp Ingest")
      templates <- sftpTemplates(idOpt, fileType, sftpPath)
    } yield templates
  }

  def hdfsIngest(fileType: String, hdfsPath: String) = {
    for{
      idOpt <- templateIdByName("HDFS & AUTOREC Ingest")
      templates <- hdfsTemplate(idOpt, fileType, hdfsPath)
    } yield templates
  }

  def trasformerIngest(fileType: String, hdfsPath: String) = {
    for{
      idOpt <- templateIdByName("Dataset Transformation")
      templates <- transformationTemplate(idOpt, fileType, hdfsPath)
    } yield templates
  }



  //WsResponse passed as parameter only fro chaining the call
  def categoryFuture(meta :MetaCatalog): Future[JsValue] = {
    val categoriesWs = ws.url(KYLOURL + "/api/v1/feedmgr/categories")
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get()

    val categoryFuture = categoriesWs.map { x =>
      val categoriesJson = x.json
      val categories = categoriesJson.as[List[JsValue]]
      val found =categories.filter(cat => {(cat \ "systemName").as[String].equals(meta.dcatapit.owner_org.get)})
      found.head
    }
    categoryFuture
  }

}
