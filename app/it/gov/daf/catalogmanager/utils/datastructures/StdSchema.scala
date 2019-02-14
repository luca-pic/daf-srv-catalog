package it.gov.daf.catalogmanager.utils.datastructures

import it.gov.daf.model.DatasetCatalog


case class StdSchema (
                       name: String = "",
                       nameDataset: String,
                       uri: String,
                       stdSchemaName: String = "",  //to be deleted
                       theme: String,
                       cat: Seq[String] = Seq(),
                       groupOwn: String,
                       owner: String,
                       dataSchema: DatasetCatalog
                     )