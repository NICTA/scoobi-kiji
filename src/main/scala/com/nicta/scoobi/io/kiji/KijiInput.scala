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
  def fromMostRecentValues[A : WireFormat : KijiFormat](instance: String, layoutPath: String, table: String, family: String, qualifier: String) : DList[EntityValue[A]] = {
    val source = KijiSource(instance, layoutPath, table, family, qualifier, MostRecentValues)(wireFormat[A], kijiFormat[A])

    val entityIdWf = EntityIdWireFormat.entityIdWireFormat(KijiTableLayout.createFromEffectiveJson(new FileInputStream(layoutPath)))
    DListImpl[EntityValue[A]](source)(EntityValue.entityValueHasWireFormat(entityIdWf, wireFormat[A]))
  }
}

object KijiInput extends KijiInput

/**
 * Scoobi DataSource for a Kiji column
 *
 * @param instance uri of the Kiji instance
 * @param layoutPath path for the table layout
 * @param tableName table name
 * @param family column family
 * @param qualifier column qualifier
 * @param access access mode, i.e. specification of the values to retrieve: most recent, in a time range
 *
 */
case class KijiSource[A : WireFormat : KijiFormat](instance: String, layoutPath: String, tableName: String,
                                                   family: String, qualifier: String, access: KijiRowAccess) extends DataSource[KijiKey, KijiRow, EntityValue[A]] {

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
    val request = buildRequest(KConstants.BEGINNING_OF_TIME, KConstants.END_OF_TIME, family, qualifier)

    job.getConfiguration.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, Base64.encodeBase64String(SerializationUtils.serialize(request)))
    job.getConfiguration.set(KijiConfKeys.KIJI_INPUT_TABLE_URI,    tableUri.map(_.toString).getOrElse(""))
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = 0

  def inputConverter: InputConverter[KijiKey, KijiRow, EntityValue[A]] = KijiInputConverter[A](family, qualifier, access)

  /**
   * build a data request for the desired column with minTime and maxTime as constraints
   */
  private def buildRequest(minTime: Long, maxTime: Long, family: String, qualifier: String) = {
    val builder = KijiDataRequest.builder.withTimeRange(minTime, maxTime)
    builder.addColumns(builder.newColumnsDef.add(family, qualifier)).build
  }

}

/**
 * The KijiInputConverter specifies which value to extract from a given Kiji data row and key
 */
case class KijiInputConverter[A : KijiFormat : WireFormat](family: String, qualifier: String, access: KijiRowAccess) extends InputConverter[KijiKey, KijiRow, EntityValue[A]] {
  def fromKeyValue(context: InputContext, key: KijiKey, row: KijiRow): EntityValue[A] = {
    if (access == MostRecentValues) EntityValue(key.get, implicitly[KijiFormat[A]].fromKiji(row.get.getMostRecentValue[A](family, qualifier)))
    else throw new Exception(s"undefined access $access")
  }
}

sealed trait KijiRowAccess {
  def getName: String
  override def equals(a: Any) = {
    a match {
      case access: KijiRowAccess => getName == access.getName
      case _ => false
    }
  }
}
object MostRecentValues extends KijiRowAccess {
  def getName = "most recent"
}

