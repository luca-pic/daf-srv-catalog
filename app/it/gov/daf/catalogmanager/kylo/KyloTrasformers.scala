package it.gov.daf.catalogmanager.kylo

import catalog_manager.yaml.MetaCatalog
import play.api.libs.functional.FunctionalBuilder
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object KyloTrasformers {


  def generateInputSftpPath(mc: MetaCatalog) :String = {
    val user = mc.operational.group_own
    val domain = mc.operational.theme
    val subDomain = mc.operational.subtheme
    val dsName = mc.dcatapit.name
    val path =  s"/home/$user/$domain/$subDomain/$dsName"
    path
  }

  def  transformTemplates(value: String): Reads[JsObject] = {
    __.json.update(
      (__ \ "value").json.put(JsString(value))
    )
  }

 // val inferKylo = """{"name":null,"description":null,"charset":"UTF-8","properties":{},"fields":[{"sampleValues":["GTT","GTT_E","GTT_F","GTT_T","FS"],"name":"agency_id","description":"","nativeDataType":null,"derivedDataType":"string","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":false,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"string"},{"sampleValues":["Gruppo Torinese Trasporti","GTT servizio extraurbano","Gruppo Torinese Trasporti","GTT Servizi Turistici","Trenitalia"],"name":"agency_name","description":"","nativeDataType":null,"derivedDataType":"string","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":false,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"string"},{"sampleValues":["http://www.gtt.to.it","http://www.gtt.to.it","http://www.gtt.to.it","http://www.gtt.to.it","http://www.trenitalia.com"],"name":"agency_url","description":"","nativeDataType":null,"derivedDataType":"string","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":false,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"string"},{"sampleValues":["Europe/Rome","Europe/Rome","Europe/Rome","Europe/Rome","Europe/Rome"],"name":"agency_timezone","description":"","nativeDataType":null,"derivedDataType":"string","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":false,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"string"},{"sampleValues":["it","it","it","it","it"],"name":"agency_lang","description":"","nativeDataType":null,"derivedDataType":"string","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":false,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"string"},{"sampleValues":["800-019152","800-019152","800-019152","800-019152","199-892021"],"name":"agency_phone","description":"","nativeDataType":null,"derivedDataType":"string","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":false,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"string"},{"sampleValues":["21","43","0","121","2"],"name":"num","description":"","nativeDataType":null,"derivedDataType":"int","primaryKey":false,"nullable":true,"modifiable":true,"dataTypeDescriptor":{"numeric":true,"date":false,"complex":false},"updatedTracker":false,"precisionScale":null,"createdTracker":false,"tags":null,"descriptionWithoutNewLines":"","dataTypeWithPrecisionAndScale":"int"}],"schemaName":null,"databaseName":null,"hiveFormat":"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n WITH SERDEPROPERTIES ( 'separatorChar' = ',' ,'escapeChar' = '\\\\' ,'quoteChar' = '\\\"') STORED AS TEXTFILE","structured":false,"id":"51ebbdc8-83fd-4e70-b49d-f36dcc995ea9"}"""

 // val inferJson = Json.parse(inferKylo)


  def buildProfiling(inferJson :JsValue) : JsArray = {
    val fields = (inferJson \ "fields").as[IndexedSeq[JsObject]]
    val profilingFields = fields.map(x => {
      val fieldName = (x \ "name").as[String]
      val feedFieldName = (x \ "name").as[String]
      Json.obj("fieldName" -> fieldName,
        "feedFieldName" -> feedFieldName,
        "index" -> false,
        "partitionColumn" -> false,
        "profile" -> false,
        "validation" -> JsArray(),
        "standardization" -> JsArray()
      )
    })
    JsArray(profilingFields)
  }

  def buildUserProperties(userProperties :Seq[JsValue], metaCatalog: MetaCatalog, fileType :String) :JsArray = {
    val userProps = userProperties.map(_.as[JsObject])
    val result = userProps.map(x => {
      val systemName = (x \ "systemName").as[String]
      systemName match {
        case "dafType" => { if (metaCatalog.operational.is_std)
                                x + ("value" -> JsString("standard"))
                            else if (fileType.equals("derived"))
                                x + ("value" -> JsString("derived"))
                            else
                                x + ("value" -> JsString("ordinary"))
                            }
        case "dafDomain" => x + ("value" -> JsString(metaCatalog.operational.theme))
        case "dafSubdomain" => x + ("value" -> JsString(metaCatalog.operational.subtheme))
        case "dafFormat" => x + ("value" -> JsString(fileType))
        case "dafOpendata" => { if (metaCatalog.dcatapit.privatex.getOrElse(true))
                                   x + ("value" -> JsBoolean(false))
                                 else
                                   x + ("value" -> JsBoolean(true))
        }
        case "dafOwner" => x + ("value" -> JsString(metaCatalog.dcatapit.author.getOrElse("")))
      }
    })
    JsArray(result)
  }

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

  def querySqlToSparkSnippet(metaCatalog: MetaCatalog) :(String, String) = {
    val specialChar = "`"
    val querySql = metaCatalog.operational.type_info.get.query_sql.get
    val queryJson: JsValue = Json.parse(metaCatalog.operational.type_info.get.query_json.get)
    val selectFields: Seq[JsValue] = (queryJson \\ "name")
    var query: String = querySql.replace("T1", "tbl10")
    val selects = selectFields.map(x => {
      val oldField = x.as[String]
      val lower = "tbl10." + specialChar + oldField.toLowerCase + specialChar
      query = query.replace(oldField, lower)
      lower
    })
    val sourcesJson = metaCatalog.operational.type_info.get.sources.get
    println(querySql)
    val startingIndex = querySql.split("\\.")
    val db = startingIndex(0).substring(startingIndex(0).lastIndexOf(" ") + 1,
      startingIndex(0).length)
    val table = startingIndex(1).substring(0,
      startingIndex(1).indexOf(" "))

    val from = specialChar + db + specialChar + "." +
      specialChar + table + specialChar

    //val startTrasformSql = """import org.apache.spark.sql._ var df = sqlContext.sql(" """
    //val endTrasformSql = """ ") df = df df """
    val startTrasformSql = "import org.apache.spark.sql._; var df = sqlContext.sql(\" "
    val endTrasformSql = " \"); df = df; df "

    query = query.replace(db + "." + table, from)
    (query, startTrasformSql + query + endTrasformSql)
  }

  //id: "efc036fe-ef47-42a6-bb00-7067efb358a5",
  //name: "DAF Category",
  //systemName: "daf_category"

  //"id": "6ef0ef5b-5c8f-42fc-9f0d-37f67430f1f5",
  //"name": "default_org",
  //"systemName": "default_org"

  def feedTrasform(metaCatalog: MetaCatalog, template :JsValue, templates : List[JsObject], inferJson :JsValue, category :JsValue, fileType :String, skipHeader :Boolean): Reads[JsObject] = __.json.update(
    (__ \ 'feedName).json.put(JsString(metaCatalog.dcatapit.holder_identifier.get + "_o_" + metaCatalog.dcatapit.name)) and
        (__ \ 'description).json.put(JsString(metaCatalog.dcatapit.name)) and
         (__ \ 'systemFeedName).json.put(JsString(metaCatalog.dcatapit.holder_identifier.get + "_o_" + metaCatalog.dcatapit.name)) and
         (__ \ 'templateId).json.put(JsString((template \ "id").get.as[String])) and
         (__ \ 'templateName).json.put(JsString((template \ "templateName").get.as[String])) and
         (__ \ 'inputProcessorType).json.put(JsString(((template \ "inputProcessors")(0) \ "type").get.as[String])) and
         (__ \ 'inputProcessorName).json.put(JsString(((template \ "inputProcessors")(0) \ "name").get.as[String])) and
         (__ \ 'properties).json.put(JsArray(templates)) and
         ((__ \ 'table) \ 'tableSchema).json.put(Json.obj("name" -> (metaCatalog.dcatapit.holder_identifier.get + "_o_" + metaCatalog.dcatapit.name)
         , "fields" -> (inferJson \ "fields").as[JsArray]  )) and
         (((__ \ 'table) \ 'sourceTableSchema) \ 'fields).json.put((inferJson \ "fields").as[JsArray]) and
         (((__ \ 'table) \ 'feedTableSchema) \ 'fields).json.put((inferJson \ "fields").as[JsArray]) and
         ((__ \ 'table) \ 'feedFormat).json.put(JsString((inferJson \ "hiveFormat").as[String])) and
         ((__ \ 'table) \ 'targetMergeStrategy).json.put(JsString(metaCatalog.operational.dataset_proc.get.merge_strategy.toUpperCase)) and
         ((__ \ 'table) \ 'fieldPolicies).json.put(buildProfiling(inferJson)) and
         ((__ \ 'schedule) \ 'schedulingStrategy).json.put(JsString(metaCatalog.operational.dataset_proc.get.scheduling_strategy.get)) and
         ((__ \ 'schedule) \ 'schedulingPeriod).json.put(JsString(metaCatalog.operational.dataset_proc.get.cron)) and
         (__ \ 'category).json.put(Json.obj("id" -> (category \ "id").as[String],
                                      "name" ->  (category \ "name").as[String],
                                      "systemName" -> (category \ "systemName").as[String])) and
         (__ \ 'dataOwner).json.put(JsString((category \ "systemName").as[String])) and
      ((__ \ 'options) \ 'skipHeader).json.put(JsBoolean(skipHeader))
         reduce
  ) andThen (__ \ 'userProperties).json.update(
    of[JsArray].map{ case JsArray(arr) => buildUserProperties(arr, metaCatalog, fileType) }
  )


  def feedTrasformationTemplate(metaCatalog: MetaCatalog, category :JsValue): Reads[JsObject] = {
    val feedName = metaCatalog.dcatapit.holder_identifier.get + "_o_" + metaCatalog.dcatapit.name
    val org = metaCatalog.dcatapit.holder_identifier.get
    val kyloSchema = Json.parse(metaCatalog.dataschema.kyloSchema.get)
    // Test for only one dependent feed
    // TODO handle multiple sources and open data
    val sourceString = metaCatalog.operational.type_info.get.sources.get.head
    val source = Json.parse(sourceString)
    val dependentOrg = (source \ "owner_org").as[String]
    val dependentName = (source \ "name").as[String]
    val dependentFeed = s"$dependentOrg.$dependentOrg" + "_o_" + dependentName
    __.json.update(
      (__ \ 'feedName).json.put(JsString(feedName)) and
        (__ \ 'systemFeedName).json.put(JsString(feedName)) and
        (__ \ 'category).json.put(
          Json.obj("id" -> (category \ "id").as[String],
            "name" -> (category \ "name").as[String],
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
                    val d = arr.map((_.as[JsObject] + ("label" -> JsString(dependentFeed)) + ("value" -> JsString(dependentFeed))))
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
          val trasformedGroup = groups.value.map(group => {
            val d = group.as[JsObject] + ("properties" -> JsArray(propsObj))
            d
          })

          val rules = (x \ "ruleType").as[JsObject] + ("properties" -> JsArray(propsObj)) + ("groups" -> JsArray(trasformedGroup))


          val result: JsValue = (x.as[JsObject] +
            ("properties" -> JsArray(propsObj))) +
            ("propertyValuesDisplayString" -> JsString(s" Dependent Feeds: $dependentFeed")) +
            ("groups" -> JsArray(trasformedGroup)) +
            ("ruleType" -> rules)
          result

        })
        JsArray(finalResults)
      }
      }
    ) andThen (__ \ "table" \ "tableSchema").json.update(
      (__ \ "name").json.put(JsString(feedName))
    ) andThen (__ \ "table" \ "tableSchema" \ "fields").json.update(
      of[JsArray].map { case JsArray(arr) => {
        val template = arr.head
        val fields = tableFields(template, kyloSchema)
        fields
      }
      }

    ) andThen (__ \ "table" \ "sourceTableSchema" \ "fields").json.update(
      of[JsArray].map { case JsArray(arr) => {
        val template = arr.head
        val fields = tableFields(template, kyloSchema)
        fields
      }
      }

    ) andThen (__ \ "table" \ "feedTableSchema" \ "fields").json.update(
      of[JsArray].map { case JsArray(arr) => {
        val template = arr.head
        val fields = tableFields(template, kyloSchema)
        fields
      }
      }

    ) andThen (__ \ "table" \ "fieldPolicies").json.update(
      of[JsArray].map { case JsArray(arr) => {
        val template = arr.head
        val fields = tablePoliciesFields(template, kyloSchema)
        fields
      }
      }

    )  andThen (__ \ "dataTransformation").json.update(
      (__ \ "sql").json.put(JsString(querySqlToSparkSnippet(metaCatalog)._1)) and
        (__ \ "dataTransformScript").json.put(JsString(querySqlToSparkSnippet(metaCatalog)._2))
    reduce
    ) andThen (__ \ "dataTransformation" \ "$selectedColumnsAndTables").json.update(
      of[JsArray].map { case JsArray(arr) => {
        val inferred = (kyloSchema \ "fields").as[JsArray]
        //val tableName = metaCatalog.operational.theme + "__" + metaCatalog.operational.subtheme + "." + dependentOrg + "_o_" + dependentName
        val result = inferred.value.map(x => {
          val name = (x \ "name").as[String]
          Json.obj(
            "column" -> name,
            "alias" -> "tbl10",
            //"tableName" -> "tran__marittimo.new_org2_o_botteghe_trento",
            // Maybe error but i believe kylo does not use it
            "tableName" -> dependentFeed,
            "tableColumn" -> name)
        })
        JsArray(result)
      }

      }

    )andThen (__ \ 'userProperties).json.update(
      of[JsArray].map{ case JsArray(arr) => buildUserProperties(arr, metaCatalog, "derived") }
    )
  }
}
