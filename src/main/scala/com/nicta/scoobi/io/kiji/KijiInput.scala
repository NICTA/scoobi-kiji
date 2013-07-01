/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  def fromRequest(table: KijiTable, request: KijiDataRequest,
                  scanOpts: ScoobiKijiScanOpts = ScoobiKijiScanOpts()) : DList[EntityRow] = {
    val source = KijiSource(table.getURI, request, scanOpts)
    DListImpl[EntityRow](source)(EntityRow.entityValueHasWireFormat(table, request))
  }
}

object KijiInput extends KijiInput

case class ScoobiKijiScanOpts(blockCaching: Boolean = false,
                              prefetch: Int = 1) { }

/**
 * Scoobi DataSource for a Kiji column
 *
 */
case class KijiSource(@transient tableUri: KijiURI,
                      @transient request: KijiDataRequest,
                      @transient scanOpts: ScoobiKijiScanOpts) extends DataSource[KijiKey, KijiRow, EntityRow] {

  private implicit lazy val logger = LogFactory.getLog("scoobi.KijiSource")

  def inputFormat: Class[_ <: InputFormat[KijiKey, KijiRow]] = classOf[KijiInputFormat]

  def inputCheck(implicit sc: ScoobiConfiguration) {}

  /**
   * During the configuration we build a request object and serialise it in the Configuration properties
   */
  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    job.getConfiguration.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST,       Base64.encodeBase64String(SerializationUtils.serialize(request)))
    job.getConfiguration.set(KijiConfKeys.KIJI_INPUT_TABLE_URI,          tableUri.toString)
    job.getConfiguration.set(KijiScoobiConfKeys.BLOCK_CACHING_KEY,       scanOpts.blockCaching.toString)
    job.getConfiguration.set(KijiScoobiConfKeys.SERVER_PREFETCH_KEY,     scanOpts.prefetch.toString)
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

