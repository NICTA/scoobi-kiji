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

/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader}

import Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema._
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import org.kiji.schema.hbase.HBaseScanOptions
import org.kiji.schema.filter.KijiRowFilter
import org.apache.commons.logging.LogFactory

import io.text.TextInput.AnInt

final class KijiRecordReader extends RecordReader[KijiKey, KijiRow] {
  private implicit lazy val logger = LogFactory.getLog("scoobi.KijiRecordReader")
  private var configuration: Configuration = new Configuration
  private var split: KijiTableSplit = _

  def initialize(s: InputSplit, context: TaskAttemptContext) {
    if (!s.isInstanceOf[KijiTableSplit]) {
      sys.error("KijiRecordReader received an InputSplit that was not a KijiTableSplit.")
    }
    split = s.asInstanceOf[KijiTableSplit]
    configuration = context.getConfiguration
  }

  private lazy val dataRequest: KijiDataRequest = {
    // Get data request from the job configuration.
    val dataRequestB64  =
      Option(configuration.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST)).
        getOrElse(sys.error(s"Missing data request in job configuration, at key ${KijiConfKeys.KIJI_INPUT_DATA_REQUEST}"))

    SerializationUtils.deserialize(Base64.decodeBase64(Bytes.toBytes(dataRequestB64))).asInstanceOf[KijiDataRequest]
  }

  private lazy val inputURI = KijiURI
    .newBuilder(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
    .build

  private lazy val reader: KijiTableReader = {
    doAndRelease(Kiji.Factory.open(inputURI, configuration)) { kiji =>
      doAndRelease(kiji.openTable(inputURI.getTable))(_.openTableReader)
    }
  }

  def b2h(bytes: Array[Byte]): String = bytes.map("%02x" format _).mkString

  private lazy val scanner: KijiRowScanner = {
    val startRow = split.getStartRow
    val endRow = split.getEndRow
    logger.info("Start key: " + b2h(startRow))
    logger.info("End key: " + b2h(endRow))

    val scannerOptions: KijiScannerOptions = new KijiScannerOptions()
      .setStartRow(HBaseEntityId.fromHBaseRowKey(startRow))
      .setStopRow(HBaseEntityId.fromHBaseRowKey(endRow))

    for { 
      filterJson <- Option(configuration.get(KijiConfKeys.KIJI_ROW_FILTER))
      filter = KijiRowFilter.toFilter(filterJson)
    } scannerOptions.setKijiRowFilter(filter)

    logger.info("Locations: " + split.getLocations().mkString(","))
    val hbaseScanOptions = new HBaseScanOptions()

    val bc = configuration.getBoolean(KijiScoobiConfKeys.BLOCK_CACHING_KEY, true)
    logger.info("Turning block cache " + (if(bc) "on" else "off"))
    hbaseScanOptions.setCacheBlocks(bc)

    val sp = configuration.getInt(KijiScoobiConfKeys.SERVER_PREFETCH_KEY, 1)
    logger.info("Setting row buffer to " + sp)
    hbaseScanOptions.setServerPrefetchSize(sp)

    scannerOptions.setHBaseScanOptions(hbaseScanOptions)

    reader.getScanner(dataRequest, scannerOptions)
  }

  private lazy val iterator: java.util.Iterator[KijiRowData] = scanner.iterator()
  private var isClosed: Boolean = false

  private var currentKey = new KijiKey()
  private var currentValue = new KijiRow
  override def getCurrentKey = currentKey
  override def getCurrentValue = currentValue
  override def getProgress = 0.0f

  override def nextKeyValue: Boolean = {
    iterator.hasNext && {
      val rowData = iterator.next
      getCurrentKey.set(rowData.getEntityId)
      getCurrentValue.set(rowData)
      true
    }
  }
  override def close() {
    if (!isClosed) {
      isClosed = true
      scanner.close()
      reader.close()
    }
  }
}

