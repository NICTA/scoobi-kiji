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
package io.kiji

import org.apache.hadoop.mapreduce._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.{KijiRegion, KijiTable, Kiji, KijiURI}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTableInterface
import org.kiji.schema.impl.HBaseKijiTable
import org.apache.hadoop.hbase.mapreduce.TableSplit
import Resources._
import scala.collection.JavaConverters._
import KijiConfKeys._
import java.io.IOException
import org.apache.hadoop.conf.{Configuration, Configurable}

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
case class KijiInputFormat() extends InputFormat[KijiKey, KijiRow] with Configurable {
  private var configuration: Configuration = _
  def setConf(conf: Configuration) { configuration = conf }
  def getConf = configuration

  private def sliding2[T](i: List[T]): List[(T, T)] = i match {
    case f :: s :: r => (f, s) :: sliding2(s :: r)
    case _ => List()
  }

  private def splitKeyRange(s: Array[Byte], e: Array[Byte], f: Int): Array[Array[Byte]] = f match {
    case 0 => Array(s, e) // Bytes.split doesn't handle the 0 case
    case i => Bytes.split(s, e, f)
  }

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = Option(configuration).getOrElse(context.getConfiguration)
    val uriString: String = Option(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).getOrElse(throw new IOException(s"There should be a $KIJI_INPUT_TABLE_URI entry in the configuration"))
    val inputTableURI: KijiURI = KijiURI.newBuilder(uriString).build()

    doAndRelease(Kiji.Factory.open(inputTableURI, conf)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(inputTableURI.getTable)) { table: KijiTable =>
        val htable: HTableInterface = HBaseKijiTable.downcast(table).getHTable

        val splitFctr = conf.getInt(KijiScoobiConfKeys.REGION_SPLIT_FACTOR_KEY, 1) - 1
        table.getRegions.asScala.flatMap { region: KijiRegion =>
          // TODO(KIJIMR-65): For now pick the first available location (ie. region server),
          //     if any.
          val location =
            if (region.getLocations.isEmpty) null
            else                             region.getLocations.iterator.next.replaceAll(":.*", "")

          val rStartKey = region.getStartKey
          val rEndKey = if(region.getEndKey.isEmpty) region.getStartKey.map(_ => 0xFF.asInstanceOf[Byte]) else region.getEndKey
          for((s, e) <- sliding2(splitKeyRange(rStartKey, rEndKey, splitFctr).toList)) yield {
            val tableSplit = new TableSplit(htable.getTableName, s, e, location)
            new KijiTableSplit(new org.kiji.mapreduce.impl.KijiTableSplit(tableSplit)).asInstanceOf[InputSplit]
          }
        }.asJava
      }
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[KijiKey, KijiRow] = {
    split match {
      // TODO: Use reporter to report progress.
      case kijiSplit: KijiTableSplit => {
        val recordReader = new KijiRecordReader()
        recordReader.initialize(kijiSplit, context)
        recordReader
      }
      case _ => sys.error("KijiInputFormat requires a KijiTableSplit.")
    }
  }

}

