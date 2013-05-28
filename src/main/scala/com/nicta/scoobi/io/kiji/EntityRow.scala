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
import core.WireFormat
import java.io.{DataInput, DataOutput}
import org.kiji.schema.layout.KijiTableLayout
import scala.collection.JavaConversions._
import org.kiji.schema.impl.{HBaseKijiTable, HBaseKijiRowData}
import org.apache.hadoop.hbase.client.Result
import KijiFormat._

/**
 * This class represents a single value from a Kiji data row.
 *
 * It has an entity id and a well-typed value
 */
case class EntityRow(entityId: EntityId, data: KijiRowData) extends Dynamic {
  def applyDynamic[T : KijiFormat](family: String)(qualifier: String): T = kijiFormat[T].fromKiji(data.getMostRecentValue[T](family, qualifier))
  def selectDynamic[T : KijiFormat](family: String)(qualifier: String): Seq[T] = data.getMostRecentValues[T](family).values.toSeq.map(kijiFormat[T].fromKiji)

  override def equals(a: Any) = a match {
    case other: EntityRow => entityId == other.entityId
    case _                => false
  }

  override def hashCode = entityId.hashCode
  override def toString = entityId.toString
}

object EntityRow {

  implicit def entityRowHasEntityId: EntityIdComponent[EntityRow] = new EntityIdComponent[EntityRow] {
    def toEntityId(v: EntityRow) = v.entityId
  }

  def entityValueHasWireFormat(table: KijiTable, request: KijiDataRequest): EntityValueWireFormat =
    entityValueHasWireFormat(table.getKiji.getURI, table.getURI, table.getLayout, request)

  implicit def entityValueHasWireFormat(instanceUri: KijiURI, tableUri: KijiURI, layout: KijiTableLayout, request: KijiDataRequest): EntityValueWireFormat =
    EntityValueWireFormat(instanceUri, tableUri, layout, request)

  case class EntityValueWireFormat(instanceUri: KijiURI, tableUri: KijiURI, layout: KijiTableLayout, request: KijiDataRequest) extends WireFormat[EntityRow] {
    implicit val entityIdWf = EntityIdWireFormat.entityIdWireFormat(layout)

    def toWire(row: EntityRow, out: DataOutput) {
      entityIdWf.toWire(row.entityId, out)

      row.data match {
        case hbaseRow: HBaseKijiRowData => Option(hbaseRow.getHBaseResult).map(_.write(out))
        case _ => ()
      }
    }

    def fromWire(in: DataInput): EntityRow = fromWire(in, Kiji.Factory.open(instanceUri).openTable(tableUri.getTable))

    def fromWire(in: DataInput, table: KijiTable): EntityRow = {
      val entityId = entityIdWf.fromWire(in)
      val result = new Result
      result.readFields(in)
      EntityRow(entityId, new HBaseKijiRowData(entityId, request, table.asInstanceOf[HBaseKijiTable], result))
    }
  }
}


/**
 * typeclass for any which can be translated to an entity id
 */
trait EntityIdComponent[T] {
  def toEntityId(t: T): EntityId
}

object EntityIdComponent {

  /**
   * for the given layout, if the entity id can be represented as a string, return the entity id from the string
   * component
   */
  implicit def stringIsEntityIdComponent(layout: KijiTableLayout): EntityIdComponent[String] = new EntityIdComponent[String] {
    def toEntityId(s: String) = EntityIdFactory.getFactory(layout).getEntityId(s)
  }
}

/**
 * Wire format for an entity id, given a specific layout.
 *
 * It is serialised as a HBase row key
 */
object EntityIdWireFormat {
  implicit def entityIdWireFormat(layout: KijiTableLayout): WireFormat[EntityId] = new WireFormat[EntityId] {
    def toWire(x: EntityId, out: DataOutput) {
      val bytes = x.getHBaseRowKey
      out.writeInt(bytes.length)
      out.write(bytes)
    }

    def fromWire(in: DataInput) = {
      val length = in.readInt()
      val bytes = new Array[Byte](length)
      in.readFully(bytes)
      EntityIdFactory.getFactory(layout).getEntityIdFromHBaseRowKey(bytes)
    }
  }
}

