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

import org.kiji.schema._
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.mapreduce.framework.{KijiConfKeys, HFileKeyValue}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, OutputFormat}
import org.kiji.mapreduce.output.framework.KijiHFileOutputFormat
import org.apache.hadoop.conf.Configuration
import org.kiji.schema.layout.impl.ColumnNameTranslator
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory
import com.nicta.scoobi.core._
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.kiji.mapreduce.output.MapReduceJobOutputs

trait KijiOutput {
  implicit class ToHFile[T](val list: DList[EntityValue[T]]) {
    def toHFile(path: String, table: KijiTable) = list.addSink(new HBaseFileSink(path, table))
  }
}
object KijiOutput extends KijiOutput

class HBaseFileSink[T](path: String, @transient table: KijiTable, check: Sink.OutputCheck = Sink.defaultOutputCheck) extends DataSink[HFileKeyValue, NullWritable, EntityValue[T]] {
  private lazy val output = new Path(path)
  private var tableUri = table.getURI.toString
  private var instanceUri = table.getKiji.getURI.toString

  private var converter: HFileKeyValueConverter[T] = _
  val KIJI_INSTANCE_URI = "kiji.instance.uri"

  def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[HFileKeyValue, NullWritable]] = classOf[KijiHFileOutputFormat]
  def outputKeyClass(implicit sc: ScoobiConfiguration) = classOf[HFileKeyValue]
  def outputValueClass(implicit sc: ScoobiConfiguration) =  classOf[NullWritable]

  override def outputSetup(implicit configuration: ScoobiConfiguration) {
    val kiji = Kiji.Factory.open(KijiURI.newBuilder(instanceUri).build)
    val layout = kiji.openTable(KijiURI.newBuilder(tableUri).build.getTable)
    converter = new HFileKeyValueConverter(kiji, layout)
  }
  def outputConverter = converter
  def outputCheck(implicit sc: ScoobiConfiguration) { outputPath(sc).foreach(path => check(path, true, sc)) }
  def outputPath(implicit sc: ScoobiConfiguration) = Some(output)
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    job.getConfiguration.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, table.getURI.toString)
    MapReduceJobOutputs.newHFileMapReduceJobOutput(table.getURI, new Path(path)).configure(job)
  }
  override def outputTeardown(implicit configuration: ScoobiConfiguration) {
    converter.table.release
    converter.kiji.release
  }

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = this
  def compression = None
}



case class EntityValue[T](key: Array[Byte], family: String, qualifier: String, timestamp: Long, value: T)

object EntityValue {
  def create[T](entityId: EntityId, family: String, qualifier: String, timestamp: Long, value: T) =
    new EntityValue[T](entityId.getHBaseRowKey, family, qualifier, timestamp, value)

  implicit def wireFormat[T : WireFormat](layout: KijiTableLayout): WireFormat[EntityValue[T]] = {
    implicit val entityIdWireFormat = EntityIdWireFormat.entityIdWireFormat(layout)
    WireFormat.mkCaseWireFormat(EntityValue.apply[T] _, EntityValue.unapply[T] _)
  }
}

class HFileKeyValueConverter[T](val kiji: Kiji, val table: KijiTable) extends OutputConverter[HFileKeyValue, NullWritable, EntityValue[T]] {
  private val columnNameTranslator = new ColumnNameTranslator(table.getLayout)

  def toKeyValue(kv: EntityValue[T])(implicit configuration: Configuration): (HFileKeyValue, NullWritable) = {
    val kijiColumn = new KijiColumnName(kv.family, kv.qualifier)
    val hbaseColumn = columnNameTranslator.toHBaseColumnName(kijiColumn)
    val cellSpec = table.getLayout.getCellSpec(kijiColumn).setSchemaTable(kiji.getSchemaTable)
    val encoder = DefaultKijiCellEncoderFactory.get.create(cellSpec)

    (new HFileKeyValue(kv.key, hbaseColumn.getFamily, hbaseColumn.getQualifier, kv.timestamp, encoder.encode(kv.value)), NullWritable.get)
  }
}
