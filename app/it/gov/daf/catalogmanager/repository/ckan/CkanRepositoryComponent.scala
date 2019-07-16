package it.gov.daf.catalogmanager.repository.ckan

import catalog_manager.yaml.{AutocompRes, Credentials, Error, Success, User}
import play.api.libs.ws.WSClient

/**
  * Created by ale on 10/05/17.
  */

import catalog_manager.yaml.{Dataset, Organization}
import play.api.libs.json.{JsResult, JsValue}

import scala.concurrent.Future

/**
  * Created by ale on 01/07/17.
  */
trait CkanRepository {

  def getMongoUser(name:String,callingUserid :Option[String]): JsResult[User]
  def verifyCredentials(credentials: Credentials):Boolean
  def updateOrganization(orgId: String, jsonOrg: JsValue,callingUserid :Option[String]): Future[String]
  def patchOrganization(orgId: String, jsonOrg: JsValue,callingUserid :Option[String]): Future[String]
  def createUser(jsonUser: JsValue,callingUserid :Option[String]): Future[String]
  def getUserOrganizations(userName :String,callingUserid :Option[String]) : Future[JsResult[Seq[Organization]]]
  def createDataset(jsonDataset: JsValue,callingUserid :Option[String]): Future[String]
  def createDatasetCkanGeo(catalog: Dataset, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]]
  def deleteDatasetCkanGeo(catalog: Dataset, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]]
  def createOrganization(jsonDataset: JsValue,callingUserid :Option[String]): Future[String]
  def dataset(datasetId: String,callingUserid :Option[String]): JsValue
  def getOrganization(orgId :String,callingUserid :Option[String]) : Future[JsResult[Organization]]
  def getOrganizations(callingUserid :Option[String]) : Future[JsValue]
  def getDatasets(callingUserid :Option[String]) : Future[JsValue]
  def searchDatasets( input: (Option[String], Option[String], Option[BigInt], Option[BigInt]), callingUserid :Option[String] ) : Future[JsResult[Seq[Dataset]]]
  def autocompleteDatasets( input: (Option[String], Option[BigInt]), callingUserid :Option[String]) : Future[JsResult[Seq[AutocompRes]]]
  def getDatasetsWithRes( input: (Option[BigInt], Option[BigInt]), callingUserid :Option[String] ) : Future[JsResult[Seq[Dataset]]]
  def testDataset(datasetId :String, callingUserid :Option[String]) : Future[JsResult[Dataset]]

}

object CkanRepository {
  def apply(config: String): CkanRepository = config match {
    case "dev" => new CkanRepositoryDev
    case "prod" => new CkanRepositoryProd
  }
}

trait CkanRepositoryComponent {
  val ckanRepository :CkanRepository
}
