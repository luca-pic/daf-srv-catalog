import java.io.FileInputStream

import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
import catalog_manager.yaml.BodyReads._
import play.api.libs.functional._
import play.api.libs.functional.FunctionalBuilder
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import play.api.libs.ws.WSAuthScheme
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import catalog_manager.yaml.{Error, MetaCatalog}
import it.gov.daf.catalogmanager.json
import play.Environment
import play.api.libs.ws._
import play.api.libs.ws.ahc.AhcWSClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



val streamKyloTemplate = new FileInputStream("/Users/ale/Development/teamdigitale/code/daf-backend/daf-srv-catalog/data/kylo/template_trasformation.json")

val kyloTemplate = try {
  Json.parse(streamKyloTemplate)
} finally {
  streamKyloTemplate.close()
}

val streamMetacalogTemplate = new FileInputStream("/Users/ale/Development/teamdigitale/code/daf-backend/daf-srv-catalog/data/kylo/tmp_metacalog_derived.json")

val metacatalogTemplate = try {
  Json.parse(streamMetacalogTemplate)
} finally {
  streamMetacalogTemplate.close()
}

val metaCatalog: MetaCatalog = metacatalogTemplate.validate[MetaCatalog].get


val a = ((kyloTemplate \ "schedule" \ "preconditions")(0) \ "properties").as[JsArray]

def simpleTrasform(test :String): Reads[JsObject] =
 // __.json.update( of[JsArray].map{ case JsArray(arr) =>  JsArray(arr) }) //buildScheduleProperties(arr,metaCatalog) })
  __.json.update( (__ \ "value").json.put(JsString("merdaccia")))

val b: Seq[JsResult[JsObject]] = a.value.map(_.transform(__.json.update(
  (__ \ "value").json.put(JsString("merdaccia"))
)))

val c = b.map(_.get)

def tableFields(template :JsValue, inferredSchema :JsValue) : JsArray = {
  val inferred = (inferredSchema \ "fields").as[JsArray]
  val result = inferred.value.zipWithIndex.map {
    case (x, y) => {
      val name = (x \ "name").as[String]
      val nativeDataType = (x \ "nativeDataType").as[String]
      val derivedDataType = (x \ "derivedDataType").as[String]
      val field = template.as[JsObject] + ("name" -> JsString(name)) +
          ("_id" -> JsNumber(y)) +
        ("dataType" -> JsString(nativeDataType)) +
        ("derivedDataType" -> JsString(derivedDataType)) +
        ("origName" -> JsString(name)) +
        ("origDataType" -> JsString(nativeDataType))
      field
    }
  }
  JsArray(result)

}

def tablePoliciesFields(template :JsValue, inferredSchema :JsValue) : JsArray = {
  val inferred = (inferredSchema \ "fields").as[JsArray]
  val result = inferred.value.zipWithIndex.map {
    case (x, y) => {
      val name = (x \ "name").as[String]
      val nativeDataType = (x \ "nativeDataType").as[String]
      val derivedDataType = (x \ "derivedDataType").as[String]
      val fieldTemplate = (template \ "field")
      val field = fieldTemplate.as[JsObject] + ("name" -> JsString(name)) +
        ("_id" -> JsNumber(y)) +
        ("dataType" -> JsString(nativeDataType)) +
        ("derivedDataType" -> JsString(derivedDataType)) +
        ("origName" -> JsString(name)) +
        ("origDataType" -> JsString(nativeDataType))
      val policie = Json.obj("name" -> name,
        "profile" -> false,
        "standardization" -> JsNull,
        "validation" -> JsNull,
        "fieldName" -> name,
        "feedFieldName" -> name,
        "field" -> field
      )
      policie
    }
  }
  JsArray(result)

}



//       (__ \ 'propertyValuesDisplayString).json.put(JsString(" Dependent Feeds: new_org2.new_org2_o_botteghe_trento"))
def feedTrasformationTemplate(metaCatalog: MetaCatalog, category :JsValue): Reads[JsObject] = {
  val feedName = metaCatalog.dcatapit.holder_identifier.get + "_o_" + metaCatalog.dcatapit.name
  val org = metaCatalog.dcatapit.holder_identifier.get
  val kyloSchema  = Json.parse(metaCatalog.dataschema.kyloSchema.get)
  __.json.update(
    (__ \ 'feedName).json.put(JsString(feedName)) and
      (__ \ 'systemFeedName).json.put(JsString(feedName)) and
      (__ \ 'category).json.put(
        Json.obj("id" -> (category \ "id").as[String],
        "name" ->  (category \ "name").as[String],
        "systemName" -> (category \ "systemName").as[String]))
      reduce
  ) andThen (__ \ "schedule" \ "preconditions").json.update(
    of[JsArray].map { case JsArray(arr) => {
       val finalResults = arr.map(x => {
        val props = (x \ "properties").as[JsArray]
        val properties = props.value.map(obj => {
          val name = (obj \ "name").as[String]
          val updated: JsResult[JsObject] = name match {
            case "Since Feed" => obj.transform(__.json.update(
              (__ \ "value").json.put(JsString(org + "." + feedName))
            ))
            case "Dependent Feeds" => obj.transform(__.json.update(
                (__ \ "values").json.update(
                  of[JsArray].map { case JsArray(arr) => {
                    val d = arr.map((_.as[JsObject] + ("label" -> JsString("new_org2.new_org2_o_botteghe_trento")) + ("value" -> JsString("new_org2.new_org2_o_botteghe_trento"))))
                    JsArray(d)
                  }
                  }
                )
            ))
            case _ => JsSuccess(obj.as[JsObject])
          }
          updated
        })
        val propsObj = properties.map(_.get)

        val groups = (x \ "groups").as[JsArray]
         val trasformedGroup= groups.value.map(group => {
            val d = group.as[JsObject] + ("properties" -> JsArray(propsObj))
           d
         })

         val rules =  (x \ "ruleType").as[JsObject] + ("properties" -> JsArray(propsObj)) + ("groups" -> JsArray(trasformedGroup))


         val result: JsValue = (x.as[JsObject] +
          ("properties" -> JsArray(propsObj))) +
          ("propertyValuesDisplayString" -> JsString(" Dependent Feeds: new_org2.new_org2_o_botteghe_trento")) +
          ("groups" -> JsArray(trasformedGroup)) +
         ("ruleType" -> rules)
         result

      })
      JsArray(finalResults)
    }
    }
  ) andThen (__ \ "table" \ "tableSchema").json.update(
    (__ \ "name").json.put(JsString(feedName))
  ) andThen(__ \ "table" \ "tableSchema" \ "fields").json.update(
    of[JsArray].map { case JsArray(arr) => {
      val template = arr.head
      val fields = tableFields(template,kyloSchema)
      fields
    }
    }

  ) andThen(__ \ "table" \ "sourceTableSchema" \ "fields").json.update(
    of[JsArray].map { case JsArray(arr) => {
      val template = arr.head
      val fields = tableFields(template,kyloSchema)
      fields
    }
    }

  )andThen(__ \ "table" \ "feedTableSchema" \ "fields").json.update(
    of[JsArray].map { case JsArray(arr) => {
      val template = arr.head
      val fields = tableFields(template,kyloSchema)
      fields
    }
    }

  ) andThen(__ \ "table" \ "fieldPolicies").json.update(
    of[JsArray].map { case JsArray(arr) => {
      val template = arr.head
      val fields = tablePoliciesFields(template,kyloSchema)
      fields
    }
    }

  ) andThen(__ \ "dataTransformation" \ "$selectedColumnsAndTables").json.update(
    of[JsArray].map { case JsArray(arr) => {
      val inferred = (kyloSchema \ "fields").as[JsArray]
      val result = inferred.value.map( x => {
        val name = (x \ "name").as[String]
        Json.obj(
        "column"-> name,
        "alias" -> "tbl10",
        "tableName"-> "tran__marittimo.new_org2_o_botteghe_trento",
        "tableColumn" -> name)
      })
      JsArray(result)
    }

    }

  )



}


val category = Json.obj("id" -> "test", "name" -> "test", "systemName" -> "test")

val result: Reads[JsObject] = feedTrasformationTemplate(metaCatalog, category)

val ale: JsValue = kyloTemplate.transform(feedTrasformationTemplate(metaCatalog, category)).get
println(Json.stringify(ale))



/*
private def extractSeparator(hiveFormat :String) : Option[String] =  {
  val sep = """'separatorChar'.=.'.*'.,'""".r.findFirstIn(hiveFormat).getOrElse(",").split(" ,")(0).replace("""\\\\""", "").replaceAll("'", "").split(" = ").last.trim
  Option(sep)
}

val inferKylo = """{\"name\":null,\"description\":null,\"charset\":\"UTF-8\",\"properties\":{},\"fields\":[{\"sampleValues\":[\"2016\",\"2016\",\"2016\",\"2016\",\"2016\",\"2016\",\"2016\",\"2016\",\"2016\"],\"name\":\"DATA\",\"description\":\"\",\"nativeDataType\":null,\"derivedDataType\":\"int\",\"primaryKey\":false,\"nullable\":true,\"modifiable\":true,\"dataTypeDescriptor\":{\"numeric\":true,\"date\":false,\"complex\":false},\"updatedTracker\":false,\"precisionScale\":null,\"createdTracker\":false,\"tags\":null,\"dataTypeWithPrecisionAndScale\":\"int\",\"descriptionWithoutNewLines\":\"\"},{\"sampleValues\":[\"01\",\"01\",\"01\",\"01\",\"01\",\"01\",\"01\",\"01\",\"01\"],\"name\":\"CODICE_REGIONE\",\"description\":\"\",\"nativeDataType\":null,\"derivedDataType\":\"int\",\"primaryKey\":false,\"nullable\":true,\"modifiable\":true,\"dataTypeDescriptor\":{\"numeric\":true,\"date\":false,\"complex\":false},\"updatedTracker\":false,\"precisionScale\":null,\"createdTracker\":false,\"tags\":null,\"dataTypeWithPrecisionAndScale\":\"int\",\"descriptionWithoutNewLines\":\"\"},{\"sampleValues\":[\"Piemonte\",\"Piemonte\",\"Piemonte\",\"Piemonte\",\"Piemonte\",\"Piemonte\",\"Piemonte\",\"Piemonte\",\"Piemonte\"],\"name\":\"NOME_REGIONE\",\"description\":\"\",\"nativeDataType\":null,\"derivedDataType\":\"string\",\"primaryKey\":false,\"nullable\":true,\"modifiable\":true,\"dataTypeDescriptor\":{\"numeric\":false,\"date\":false,\"complex\":false},\"updatedTracker\":false,\"precisionScale\":null,\"createdTracker\":false,\"tags\":null,\"dataTypeWithPrecisionAndScale\":\"string\",\"descriptionWithoutNewLines\":\"\"},{\"sampleValues\":[\"111\",\"112\",\"113\",\"114\",\"115\",\"116\",\"117\",\"118\",\"119\"],\"name\":\"CODICE_TIPOLOGIA_PERSONALE\",\"description\":\"\",\"nativeDataType\":null,\"derivedDataType\":\"int\",\"primaryKey\":false,\"nullable\":true,\"modifiable\":true,\"dataTypeDescriptor\":{\"numeric\":true,\"date\":false,\"complex\":false},\"updatedTracker\":false,\"precisionScale\":null,\"createdTracker\":false,\"tags\":null,\"dataTypeWithPrecisionAndScale\":\"int\",\"descriptionWithoutNewLines\":\"\"},{\"sampleValues\":[\"Dipendente dall'Ente a tempo determinato maschio - numero\",\"Dipendente dall'Ente a tempo determinato femmina - numero\",\"Dipendente dall'Ente a tempo determinato maschio - mesi lavorati\",\"Dipendente dall'Ente a tempo determinato femmina - mesi lavorati\",\"Dipendente dalla Regione a tempo determinato maschio - numero\",\"Dipendente dalla Regione a tempo determinato femmina - numero\",\"Dipendente dalla Regione a tempo determinato maschio - mesi lavorati\",\"Dipendente dalla Regione a tempo determinato femmina - mesi lavorati\",\"Dipendente dall'ateneo a tempo determinato maschio - numero\"],\"name\":\"DESCRIZIONE_TIPOLOGIA_PERSONALE\",\"description\":\"\",\"nativeDataType\":null,\"derivedDataType\":\"string\",\"primaryKey\":false,\"nullable\":true,\"modifiable\":true,\"dataTypeDescriptor\":{\"numeric\":false,\"date\":false,\"complex\":false},\"updatedTracker\":false,\"precisionScale\":null,\"createdTracker\":false,\"tags\":null,\"dataTypeWithPrecisionAndScale\":\"string\",\"descriptionWithoutNewLines\":\"\"},{\"sampleValues\":[\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\"],\"name\":\"NUMERO\",\"description\":\"\",\"nativeDataType\":null,\"derivedDataType\":\"int\",\"primaryKey\":false,\"nullable\":true,\"modifiable\":true,\"dataTypeDescriptor\":{\"numeric\":true,\"date\":false,\"complex\":false},\"updatedTracker\":false,\"precisionScale\":null,\"createdTracker\":false,\"tags\":null,\"dataTypeWithPrecisionAndScale\":\"int\",\"descriptionWithoutNewLines\":\"\"}],\"schemaName\":null,\"databaseName\":null,\"hiveFormat\":\"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\\n WITH SERDEPROPERTIES ( 'separatorChar' = '\\\\;' ,'escapeChar' = '\\\\\\\\' ,'quoteChar' = '\\\\\\\"') STORED AS TEXTFILE\",\"structured\":false,\"id\":\"37f125c9-b29a-429d-9291-3bc85859a2fa\"}"""

// val test = Json.parse(inferKylo)

val sep = extractSeparator(inferKylo)





val categoriesString = """[ { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "7a05d086-70e6-4c3b-b53c-a209bde07902", "name": "Open Data", "systemName": "open_data", "icon": "assessment", "iconColor": "#66BB6A", "description": null, "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1515425000995, "updateDate": 1515425216116 }, { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "3f776894-fd49-4865-a55c-80e60403da9f", "name": "roma_test", "systemName": "roma_test", "icon": null, "iconColor": null, "description": null, "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1516214069556, "updateDate": 1516214069556 }, { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "a021ba49-68b4-4813-b0af-a8b18f591ccd", "name": "test_json", "systemName": "test_json", "icon": null, "iconColor": null, "description": null, "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1515668890987, "updateDate": 1516202943161 }, { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "6ef0ef5b-5c8f-42fc-9f0d-37f67430f1f5", "name": "default_org", "systemName": "default_org", "icon": "brightness_5", "iconColor": "#FF8A65", "description": null, "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1514977170821, "updateDate": 1516214196551 }, { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "ac148153-e54f-41f6-b9f4-f1c190eb0d5f", "name": "System", "systemName": "system", "icon": "cloud", "iconColor": "#FFCA28", "description": "System Data", "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1512127244328, "updateDate": 1512127246778 }, { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "a453f503-d3b8-43ff-a43a-7ac888746b52", "name": "pac_agenziaentrate", "systemName": "pac_agenziaentrate", "icon": "account_balance_wallet", "iconColor": "#FF5252", "description": "pubblica amministrazione agenzia delle entrate", "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1515510352934, "updateDate": 1515512167365 }, { "owner": { "displayName": "Data Lake Administrator", "email": null, "enabled": true, "groups": [ "admin", "user" ], "systemName": "dladmin" }, "allowedActions": { "actions": [ { "systemName": "accessCategory", "actions": [ { "systemName": "editCategorySummary" }, { "systemName": "accessCategoryDetails", "actions": [ { "systemName": "editCategoryDetails" }, { "systemName": "deleteCategory" } ] }, { "systemName": "createFeedUnderCategory" }, { "systemName": "changeCategoryPermissions" } ] } ] }, "roleMemberships": [], "feedRoleMemberships": [], "id": "f8209368-54dc-4ba2-874a-900ac4ec5978", "name": "pac_cdc", "systemName": "pac_cdc", "icon": "image_aspect_ratio", "iconColor": "#66BB6A", "description": "pac_cdc", "securityGroups": [], "userFields": [], "userProperties": [], "relatedFeeds": 0, "createDate": 1513868513661, "updateDate": 1515514382193 } ]"""

val categoriesJson = Json.parse(categoriesString)

val categories = categoriesJson.as[List[JsValue]]
val found =categories.filter(cat => {(cat \ "systemName").as[String].equals("roma_test")})
(found.head \ "id").as[String]
*/


