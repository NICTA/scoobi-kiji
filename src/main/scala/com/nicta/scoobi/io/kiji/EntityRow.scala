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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/**
 * This class represents a single value from a Kiji data row.
 *
 * It has an entity id and a well-typed value
 */
case class EntityRow(entityId: EntityId, data: KijiRowData) extends Dynamic {
  def applyDynamic[T](family: String)(qualifier: String): T = data.getMostRecentValue(family, qualifier)
  def selectDynamic[T](family: String)(qualifier: String): Seq[T] = data.getMostRecentValues(family).values.toSeq

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

  implicit def entityValueHasWireFormat(layout: KijiTableLayout, table: KijiTable, request: KijiDataRequest): WireFormat[EntityRow] = new WireFormat[EntityRow] {
    implicit val entityIdWf = EntityIdWireFormat.entityIdWireFormat(layout)

    def toWire(row: EntityRow, out: DataOutput) {
      entityIdWf.toWire(row.entityId, out)

      row.data match {
        case hbaseRow: HBaseKijiRowData => Option(hbaseRow.getHBaseResult).flatMap(result => Option(result.getBytes).map(_.write(out)))
        case _ => ()
      }
    }

    def fromWire(in: DataInput) = {
      val entityId = entityIdWf.fromWire(in)
      table match {
        case hbaseTable: HBaseKijiTable => {
          val bytes = new ImmutableBytesWritable
          bytes.readFields(in)
          val result = new Result(bytes)
          EntityRow(entityId, new HBaseKijiRowData(entityId, request, hbaseTable, result))
        }
        case other => throw new Exception(s"can't deserialise a row for the table ${table.getName}")
      }
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

