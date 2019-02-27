package it.gov.daf.catalogmanager.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl.{search, termsAgg}
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import it.gov.daf.config.ElasticsearchConfig
import it.gov.daf.model.{Error, LinkedDataset, LinkedParams, MetaCatalog}
import javax.inject.Inject
import play.api.{Configuration, Logger}
import io.circe.parser._
import io.circe.generic.auto._
import it.gov.daf.common.config.ConfigReadException

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class ElasticsearchRepository @Inject()(implicit configuration: Configuration) {

  val elasticsearchConfigConfig: ElasticsearchConfig = ElasticsearchConfig.reader.read(configuration) match {
    case Failure(error)  => throw ConfigReadException(s"Unable to read [elasticsearch proxy config]", error)
    case Success(config) => config
  }

  private val elasticsearchHost = elasticsearchConfigConfig.host
  private val elasticsearchPort = elasticsearchConfigConfig.port

  private val logger = Logger(this.getClass.getName)



  def getTag: Future[Either[Error, Seq[String]]] = {

    Logger.logger.debug(s"elasticsearchUrl: $elasticsearchHost elasticsearchPort: $elasticsearchPort")

    val client = HttpClient(ElasticsearchClientUri(elasticsearchHost, elasticsearchPort))
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

    response.map{seq =>
      if(seq.isEmpty) Left(Error(Some(404), "tags not found", None))
      else Right(seq)
    }
  }

  def getLinkedDatasets(datasetName: String, linkedParams: LinkedParams, user: String, groups: List[String], limit: Option[Int]): Future[Either[Error, Seq[LinkedDataset]]] = {

    Logger.logger.debug(s"elasticsearchUrl: $elasticsearchHost elasticsearchPort: $elasticsearchPort")

    val client = HttpClient(ElasticsearchClientUri(elasticsearchHost, elasticsearchPort))
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
          decode[MetaCatalog](source.sourceAsString) match {
            case Right(metaCatalog) => LinkedDataset("derived", metaCatalog)
          }
        }.toList
      }
    }

    def getSourcesDataset = {
      client.execute(queryElastic(searchType, querySourcesFieldName, linkedParams.sourcesName.mkString(" "), limit)).map { res =>
        res.hits.hits.map { source =>
          decode[MetaCatalog](source.sourceAsString) match {
            case Right(metaCatalog) => LinkedDataset("source", metaCatalog)
          }
        }.toList
      }
    }

    val futureDatasetLinkedList = for{
      sourcesDataset <- getSourcesDataset
      derivedDataset <- getDerivedDataset
    } yield sourcesDataset ::: derivedDataset

    futureDatasetLinkedList.map{ datasetLinkedList =>
      if(datasetLinkedList.isEmpty) Left(Error(Some(404), "Dataset linked not found", None))
      else Right(datasetLinkedList)
    }
  }



}
