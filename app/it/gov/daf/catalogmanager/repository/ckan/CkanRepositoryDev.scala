package it.gov.daf.catalogmanager.repository.ckan

import java.io.{FileInputStream, PrintWriter}

import catalog_manager.yaml.{AutocompRes, Credentials, Dataset, Error, Organization, Success, User}
import play.Environment
import play.api.libs.json._
import play.api.libs.ws.WSClient

import scala.concurrent.Future

/**
  * Created by ale on 10/05/17.
  */
class CkanRepositoryDev extends CkanRepository{


  import scala.concurrent.ExecutionContext.Implicits.global

  private def readDataset():JsValue = {
    val streamDataset = new FileInputStream(Environment.simple().getFile("data/Dataset.json"))
    try {
      Json.parse(streamDataset)
    }catch {
      case tr: Throwable => tr.printStackTrace(); JsString("Empty file")
    }finally {
      streamDataset.close()
    }
  }

  private val datasetWriter = new PrintWriter(Environment.simple().getFile("data/Dataset.json"))

  def createDataset( jsonDataset: JsValue, callingUserid :Option[String] ): Future[String] = try {
    datasetWriter.println(jsonDataset.toString)
    Future("ok")
  } finally {
    datasetWriter.flush()
  }

  def createDatasetCkanGeo(catalog: Dataset, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]] = {
    Future.successful(Right(Success("success", None)))
  }

  def deleteDatasetCkanGeo(catalog: Dataset, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]] = {
    Future.successful(Right(Success("success", None)))
  }

  def getMongoUser(name:String,callingUserid :Option[String]): JsResult[User]={
    JsSuccess(null)
  }

  def verifyCredentials(credentials: Credentials):Boolean = {
    true
  }

  def updateOrganization(orgId: String, jsonOrg: JsValue, callingUserid :Option[String]): Future[String] = {
    Future("todo")
  }

  def patchOrganization(orgId: String, jsonOrg: JsValue, callingUserid :Option[String]): Future[String] = {
    Future("todo")
  }

  def createOrganization( jsonDataset: JsValue, callingUserid :Option[String] ) : Future[String] = {
    Future("todo")
  }

  def createUser(jsonUser: JsValue, callingUserid :Option[String]): Future[String]= {
    Future("todo")
  }

  def getUserOrganizations(userName :String, callingUserid :Option[String]) : Future[JsResult[Seq[Organization]]] = {
    Future(null)
  }


  def dataset(datasetId: String, callingUserid :Option[String]): JsValue = {
    readDataset()
  }

  def getOrganization(orgId :String, callingUserid :Option[String]) : Future[JsResult[Organization]] = {
    Future(null)
  }

  def getOrganizations(callingUserid :Option[String]) : Future[JsValue] = {
    Future(null)
  }

  def getDatasets(callingUserid :Option[String]) : Future[JsValue] = {
    Future(null)
  }

  def searchDatasets( input: (Option[String], Option[String], Option[BigInt], Option[BigInt]), callingUserid :Option[String] ) : Future[JsResult[Seq[Dataset]]]={
    Future(null)
  }

  def autocompleteDatasets( input: (Option[String], Option[BigInt]), callingUserid :Option[String]) : Future[JsResult[Seq[AutocompRes]]] = {
    Future(null)
  }

  def getDatasetsWithRes( input: (Option[BigInt], Option[BigInt]), callingUserid :Option[String] ) : Future[JsResult[Seq[Dataset]]] = {
    Future(null)
  }

  def testDataset(datasetId :String, callingUserid :Option[String]) : Future[JsResult[Dataset]] = {
    Future(null)
    /*
    Future(JsSuccess(Dataset(None,None,None,None,None,
      None,None,None,None,None,None,None,
      None,None,None,None,None,
      None,None,None,None,None)))*/
  }

}
