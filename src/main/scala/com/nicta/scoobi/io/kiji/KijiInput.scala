package com.nicta.scoobi
package io
package kiji

import core._
import impl.plan.DListImpl
import java.io.{FileInputStream, IOException}
import org.apache.hadoop.mapreduce._
import org.kiji.schema._
import layout.KijiTableLayout
import org.kiji.mapreduce.framework.KijiConfKeys
import WireFormat._
import KijiFormat._
import org.apache.commons.lang.SerializationUtils
import org.apache.commons.codec.binary.Base64
import org.apache.commons.logging.LogFactory

/**
 * Methods for creating DLists from Kiji tables and columns
 */
trait KijiInput {
  def fromRequest(instance: String, layoutPath: String, tableName: String, request: KijiDataRequest) : DList[EntityRow] = {
    val source = KijiSource(instance, layoutPath, tableName, request)

    val layout = KijiTableLayout.createFromEffectiveJson(new FileInputStream(layoutPath))
    val kiji = Kiji.Factory.open(KijiURI.newBuilder(instance).build)
    val table = kiji.openTable(tableName); table.release
    kiji.release
    DListImpl[EntityRow](source)(EntityRow.entityValueHasWireFormat(layout, table, request))
  }
}

object KijiInput extends KijiInput

/**
 * Scoobi DataSource for a Kiji column
 *
 * @param instance uri of the Kiji instance
 * @param layoutPath path for the table layout
 *
 */
case class KijiSource(instance: String, layoutPath: String, tableName: String, request: KijiDataRequest) extends DataSource[KijiKey, KijiRow, EntityRow] {

  private implicit lazy val logger = LogFactory.getLog("scoobi.KijiSource")

  /**
   * get the table URI by opening the Kiji instance and getting the table from its table name
   */
  lazy val tableUri: Option[KijiURI] = {
    var kiji: Kiji = null
    try {
      kiji = Kiji.Factory.open(KijiURI.newBuilder(instance).build)
      var table: KijiTable = null
      try {
        table = kiji.openTable(tableName)
        Option(table.getURI)
      } catch { case e: Throwable => logger.error(e); None } finally Option(table).foreach(_.release)
    } catch { case e: Throwable => logger.error(e); None} finally Option(kiji).foreach(_.release)
  }

  def inputFormat: Class[_ <: InputFormat[KijiKey, KijiRow]] = classOf[KijiInputFormat]

  def inputCheck(implicit sc: ScoobiConfiguration) {
    if (!tableUri.isDefined) throw new IOException(s"Table URI $tableName does not exist.")
  }

  /**
   * During the configuration we build a request object and serialise it in the Configuration properties
   */
  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    job.getConfiguration.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, Base64.encodeBase64String(SerializationUtils.serialize(request)))
    job.getConfiguration.set(KijiConfKeys.KIJI_INPUT_TABLE_URI,    tableUri.map(_.toString).getOrElse(""))
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = 0

  def inputConverter: InputConverter[KijiKey, KijiRow, EntityRow] = KijiInputConverter(request)

}

/**
 * The KijiInputConverter specifies which value to extract from a given Kiji data row and key
 */
case class KijiInputConverter(request: KijiDataRequest) extends InputConverter[KijiKey, KijiRow, EntityRow] {
  def fromKeyValue(context: InputContext, key: KijiKey, row: KijiRow): EntityRow = EntityRow(key.get, row.get)
}

