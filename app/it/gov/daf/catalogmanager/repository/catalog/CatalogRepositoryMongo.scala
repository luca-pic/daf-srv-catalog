package it.gov.daf.catalogmanager.repository.catalog

import catalog_manager.yaml.{Dataset, DatasetNameFields, Error, LinkedDataset, LinkedParams, MetaCatalog, MetadataCat, ResponseWrites, Success}
import com.mongodb
import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import com.mongodb.casbah.Imports._
import com.sksamuel.elastic4s.http.ElasticDsl.{search, termsAgg}
import it.gov.daf.catalogmanager.utilities.{CatalogManager, ConfigReader}
import play.api.libs.ws.WSClient
import play.api.Logger
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient

import scala.concurrent.Future

/**
  * Created by ale on 18/05/17.
  */
class CatalogRepositoryMongo extends  CatalogRepository{

  private val mongoHost: String = ConfigReader.getDbHost
  private val mongoPort = ConfigReader.getDbPort
  private val database = ConfigReader.getDbHost

  private val userName = ConfigReader.userName
  private val source = ConfigReader.database
  private val password = ConfigReader.password

  private val elasticsearchUrl = ConfigReader.getElasticsearchUrl
  private val elasticsearchPort = ConfigReader.getElasticsearchPort

  private val DATIPUBBLICI_HOST = ConfigReader.datipubbliciHost

  val server = new ServerAddress(mongoHost, 27017)
  val credentials = MongoCredential.createCredential(userName, source, password.toCharArray)

  import scala.concurrent.ExecutionContext.Implicits.global
  import catalog_manager.yaml.BodyReads._

  def listCatalogs(page :Option[Int], limit :Option[Int]) :Seq[MetaCatalog] = {

    val mongoClient = MongoClient(server, List(credentials))
    //val mongoClient = MongoClient(mongoHost, mongoPort)
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val results = coll.find()
        .skip(page.getOrElse(0))
        .limit(limit.getOrElse(200))
        .toList
    mongoClient.close
    val jsonString = com.mongodb.util.JSON.serialize(results)
    val json = Json.parse(jsonString) //.as[List[JsObject]]
    val metaCatalogJs = json.validate[Seq[MetaCatalog]]
    val metaCatalog = metaCatalogJs match {
      case s: JsSuccess[Seq[MetaCatalog]] => s.get
      case e: JsError => Seq()
    }
    metaCatalog
    // Seq(MetaCatalog(None,None,None))
  }


  def catalog(logicalUri :String): Option[MetaCatalog] = {
    //val objectId : ObjectId = new ObjectId(catalogId)
    val query = MongoDBObject("operational.logical_uri" -> logicalUri)
    // val mongoClient = MongoClient(mongoHost, mongoPort)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val result = coll.findOne(query)
    mongoClient.close
    val metaCatalog: Option[MetaCatalog] = result match {
      case Some(x) => {
        val jsonString = com.mongodb.util.JSON.serialize(x)
        val json = Json.parse(jsonString) //.as[List[JsObject]]
        val metaCatalogJs = json.validate[MetaCatalog]
        val metaCatalog = metaCatalogJs match {
          case s: JsSuccess[MetaCatalog] => Some(s.get)
          case _: JsError => None
        }
        metaCatalog
      }
      case _ => None
    }
    metaCatalog
  }

  def internalCatalogByName(name: String) = {
    val query = MongoDBObject("dcatapit.name" -> name)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val result = coll.findOne(query)
    mongoClient.close()
    result match {
      case Some(catalog) => {
        val jsonString = com.mongodb.util.JSON.serialize(catalog)
        val json = Json.parse(jsonString)
        val metaCatalogJs = json.validate[MetaCatalog]
        metaCatalogJs match {
          case s: JsSuccess[MetaCatalog] => Some(s.get)
          case _: JsError => None
        }
      }
      case _ => None
    }
  }

  def catalogByName(name :String, user: String, groups: List[String]): Option[MetaCatalog] = {
    import mongodb.casbah.query.Imports._

    val queryPrivate = $and(
      MongoDBObject("dcatapit.name" -> name),
      $or(
        $and(MongoDBObject("dcatapit.privatex" -> true), MongoDBObject("dcatapit.author" -> user)),
        "operational.acl.groupName" $in groups
      )
    )
    val queryPub = $and(MongoDBObject("dcatapit.name" -> name), MongoDBObject("dcatapit.privatex" -> false))
    val query = $or(queryPub, queryPrivate)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val result = coll.findOne(query)
    mongoClient.close
    val metaCatalog: Option[MetaCatalog] = result match {
      case Some(x) => {
        val jsonString = com.mongodb.util.JSON.serialize(x)
        val json = Json.parse(jsonString) //.as[List[JsObject]]
        val metaCatalogJs = json.validate[MetaCatalog]
        val metaCatalog = metaCatalogJs match {
          case s: JsSuccess[MetaCatalog] => Some(s.get)
          case _: JsError => None
        }
        metaCatalog
      }
      case _ => None
    }
    metaCatalog
  }

  def deleteCatalogByName(nameCatalog: String, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]] = {
    import mongodb.casbah.query.Imports.$and
    import mongodb.casbah.commons.Imports._

    val widgetsResp = wsClient.url(DATIPUBBLICI_HOST + "/dati-gov/v1/dashboard/iframesByName/" + nameCatalog)
      .withHeaders("Authorization" -> token)
      .get()

    widgetsResp.map{ res =>
      if(res.status == 200 && !res.body.equals("[]")) Left(Error(s"is not possible delete catalog $nameCatalog, it has some widgets", Some(403), None))
      else if(res.status == 200) {
        val query = $and(MongoDBObject("dcatapit.name" -> nameCatalog), MongoDBObject("dcatapit.author" -> user), "operational.acl.groupName" $exists false)
        val mongoClient = MongoClient(server, List(credentials))
        val db = mongoClient(source)
        val coll = db("catalog_test")
        val result = if(coll.remove(query).getN > 0) Right(Success(s"catalog $nameCatalog deleted", None)) else Left(Error(s"catalog $nameCatalog not found", Some(404), None))
        mongoClient.close()
        Logger.logger.debug(s"$user deleted $nameCatalog from catalog_test result: ${result.isRight}")
        result
      } else {Logger.logger.debug("connection error");Left(Error(s"connection error", Some(500), None))}
    }
  }

  def publicCatalogByName(name :String): Option[MetaCatalog] = {
    //val objectId : ObjectId = new ObjectId(catalogId)
    import mongodb.casbah.query.Imports.$and

    val query = $and(MongoDBObject("dcatapit.name" -> name), MongoDBObject("dcatapit.privatex" -> false))
    // val mongoClient = MongoClient(mongoHost, mongoPort)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val result = coll.findOne(query)
    mongoClient.close
    val metaCatalog: Option[MetaCatalog] = result match {
      case Some(x) => {
        val jsonString = com.mongodb.util.JSON.serialize(x)
        val json = Json.parse(jsonString) //.as[List[JsObject]]
        val metaCatalogJs = json.validate[MetaCatalog]
        val metaCatalog = metaCatalogJs match {
          case s: JsSuccess[MetaCatalog] => Some(s.get)
          case _: JsError => None
        }
        metaCatalog
      }
      case _ => None
    }
    metaCatalog
  }

  def createCatalogExtOpenData(metaCatalog: MetaCatalog, callingUserid :MetadataCat, ws :WSClient) :Success = {

    import catalog_manager.yaml.ResponseWrites.MetaCatalogWrites

    //val mongoClient = MongoClient(mongoHost, mongoPort)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")

    val dcatapit: Dataset = metaCatalog.dcatapit
    val datasetJs : JsValue = ResponseWrites.DatasetWrites.writes(dcatapit)

    val msg = if(metaCatalog.operational.std_schema.isDefined) {
      val stdUri = metaCatalog.operational.std_schema.get.std_uri
      //TODO Review logic
      val stdCatalot: MetaCatalog = catalog(stdUri).get
      val res: Option[MetaCatalog] = CatalogManager.writeOrdinaryWithStandard(metaCatalog, stdCatalot)
      val message = res match {
        case Some(meta) =>
          val json: JsValue = MetaCatalogWrites.writes(meta)
          val obj = com.mongodb.util.JSON.parse(json.toString()).asInstanceOf[DBObject]
          val inserted = coll.insert(obj)
          mongoClient.close()
          val msg = meta.operational.logical_uri.getOrElse("")
          msg
        case _ =>
          println("Error");
          val msg = "Error"
          msg
      }
      message
    } else {
      val random = scala.util.Random
      val id = random.nextInt(1000).toString
      val res: Option[MetaCatalog]= (CatalogManager.writeOrdAndStdOrDerived(metaCatalog))
      val message = res match {
        case Some(meta) =>
          val json: JsValue = MetaCatalogWrites.writes(meta)
          val obj = com.mongodb.util.JSON.parse(json.toString()).asInstanceOf[DBObject]
          val inserted = coll.insert(obj)
          val msg = meta.operational.logical_uri.getOrElse("")
          msg
        case _ =>
          println("Error");
          val msg = "Error"
          msg
      }
      message
    }

    Success(msg, Some(msg))
  }

  def createCatalog(metaCatalog: MetaCatalog, callingUserid :MetadataCat, ws :WSClient): Either[Error, Success] = {

    import catalog_manager.yaml.ResponseWrites.MetaCatalogWrites

    //val mongoClient = MongoClient(mongoHost, mongoPort)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")

//    val dcatapit: Dataset = metaCatalog.dcatapit
//    val datasetJs : JsValue = ResponseWrites.DatasetWrites.writes(dcatapit)


    // TODO think if private should go in ckan or not as backup of metadata
//    if(!metaCatalog.dcatapit.privatex.getOrElse(true))
//      CkanRegistry.ckanRepository.createDataset(datasetJs,callingUserid)

    // val result: Future[String] =

    val msg = if(metaCatalog.operational.std_schema.isDefined) {
      val stdUri = metaCatalog.operational.std_schema.get.std_uri
      //TODO Review logic
      val stdCatalot: MetaCatalog = catalog(stdUri).get
      val res: Option[MetaCatalog] = CatalogManager.writeOrdinaryWithStandard(metaCatalog, stdCatalot)
      val message = res match {
        case Some(meta) =>
          val json: JsValue = MetaCatalogWrites.writes(meta)
          val obj = com.mongodb.util.JSON.parse(json.toString()).asInstanceOf[DBObject]
          val inserted = coll.insert(obj)
          mongoClient.close()
          val msg = meta.operational.logical_uri.getOrElse("")
          msg
        case _ =>
          println("Error");
          val msg = "Error"
          msg
      }
      message
    } else {
      val random = scala.util.Random
      val id = random.nextInt(1000).toString
      val res: Option[MetaCatalog]= (CatalogManager.writeOrdAndStdOrDerived(metaCatalog))
      val message: String = res match {
        case Some(meta) =>
          val json: JsValue = MetaCatalogWrites.writes(meta)
          val obj = com.mongodb.util.JSON.parse(json.toString()).asInstanceOf[DBObject]
          val inserted = coll.insert(obj)
          val msg = meta.operational.logical_uri.getOrElse("")
          msg
        case _ =>
          println("Error");
          val msg = "Error"
          msg
      }
      message
    }
    if(msg.equals("Error")) Left(Error("error in add catalog", Some(500), None))
    else Right(Success(msg, Some(msg)))
  }


  // Not used implement query on db
  def standardUris(): List[String] = List("raf", "org", "cert")

  def isDatasetOnCatalog(name :String) = {
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val query = "dcatapit.name" $eq name
    val results = coll.findOne(query)
    results match {
      case Some(_) => Some(true)
      case None => None
    }
  }

  def getFieldsVoc: Future[Seq[DatasetNameFields]] = {
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val result = coll.find(MongoDBObject("operational.is_vocabulary" -> true)).toList
    mongoClient.close()
    val jsonString = com.mongodb.util.JSON.serialize(result)
    val json = Json.parse(jsonString)
    val metaCatalogJs = json.validate[Seq[MetaCatalog]]
    val metaCatalog: Seq[MetaCatalog] = metaCatalogJs match {
      case s: JsSuccess[Seq[MetaCatalog]] => s.get
      case e: JsError => Seq()
    }
    Future.successful(metaCatalog.map{catalog => DatasetNameFields(catalog.dcatapit.name, catalog.dataschema.avro.fields.get.map(f => f.name))})
  }

  def getDatasetStandardFields(user: String, groups: List[String]): Future[Seq[DatasetNameFields]] = {
    import mongodb.casbah.query.Imports._

    val query = $and(
      $or(MongoDBObject("dcatapit.author" -> user), "operational.acl.groupName" $in groups),
      MongoDBObject("operational.is_std" -> true)
    )
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient(source)
    val coll = db("catalog_test")
    val results = coll.find(query).toList
    mongoClient.close
    val jsonString = com.mongodb.util.JSON.serialize(results)
    val json = Json.parse(jsonString)
    val metaCatalogJs = json.validate[Seq[MetaCatalog]]
    val metaCatalog = metaCatalogJs match {
      case s: JsSuccess[Seq[MetaCatalog]] => s.get
      case e: JsError => Seq()
    }
    Future.successful(metaCatalog.map{catalog => DatasetNameFields(catalog.dcatapit.name, catalog.dataschema.avro.fields.get.map(f => f.name))})
  }

  def getTag: Future[Seq[String]] = {

    Logger.logger.debug(s"elasticsearchUrl: $elasticsearchUrl elasticsearchPort: $elasticsearchPort")

    val client = HttpClient(ElasticsearchClientUri(elasticsearchUrl, elasticsearchPort))
    val index = "ckan"
    val searchType = "catalog_test"
    val fieldsTags = "dataschema.flatSchema.metadata.tag.keyword"
    val datasetTags = "dcatapit.tags.name.keyword"

    val query = search(index).types(searchType)
      .fetchSource(false)
      .aggregations(
        termsAgg("tagDatasets", datasetTags), termsAgg("tagFields", fieldsTags)
      )

    val searchResponse: Future[SearchResponse] = client.execute(query)

    val response: Future[Seq[String]] = searchResponse.map{ res =>
      res.aggregations.flatMap{aggr =>
        aggr._2.asInstanceOf[Map[String, Int]]("buckets").asInstanceOf[List[Map[String, AnyVal]]].map(elem => elem("key").asInstanceOf[String])
      }.toSeq.distinct
    }

    response onComplete {
      res => Logger.debug(s"found ${res.getOrElse(Seq("no_tags")).size} tags")
    }

    response
  }

  def getLinkedDatasets(datasetName: String, linkedParams: LinkedParams, user: String, groups: List[String], limit: Option[Int]): Future[Seq[LinkedDataset]] = {
    import catalog_manager.yaml.BodyReads.MetaCatalogReads

    Logger.logger.debug(s"elasticsearchUrl: $elasticsearchUrl elasticsearchPort: $elasticsearchPort")

    val client = HttpClient(ElasticsearchClientUri(elasticsearchUrl, elasticsearchPort))
    val index = "ckan"
    val searchType = "catalog_test"
    val queryDerivedFieldName = "operational.type_info.sources"
    val querySourcesFieldName = "dcatapit.name"

    def queryElastic(searchTypeInQuery: String, fieldName: String, fieldValue: String, limitResult: Option[Int]) = {
      search(index).types(searchTypeInQuery).query(
        boolQuery()
          must(
            must(matchQuery(fieldName, fieldValue))
            should(
              must(termQuery("dcatapit.privatex", true), matchQuery("operational.acl.groupName", groups.mkString(" ")).operator("OR")),
              must(termQuery("dcatapit.privatex", true), termQuery("dcatapit.author", user)),
              termQuery("dcatapit.privatex", false),
              termQuery("private", false)
            )
          )
      ).limit(limitResult.getOrElse(1000))
    }

    def getDerivedDataset = {
      client.execute(queryElastic(searchType, queryDerivedFieldName, datasetName, limit)).map { res =>
        res.hits.hits.map { source =>
          MetaCatalogReads.reads(Json.parse(source.sourceAsString)) match {
            case JsSuccess(value, _) => LinkedDataset("derived", value)
          }
        }.toList
      }
    }

    def getSourcesDataset = {
      client.execute(queryElastic(searchType, querySourcesFieldName, linkedParams.sourcesName.mkString(" "), limit)).map { res =>
        res.hits.hits.map { source =>
          MetaCatalogReads.reads(Json.parse(source.sourceAsString)) match {
            case JsSuccess(value, _) => LinkedDataset("source", value)
          }
        }.toList
      }
    }

    for{
      sourcesDataset <- getSourcesDataset
      derivedDataset <- getDerivedDataset
    } yield sourcesDataset ::: derivedDataset
  }


}
