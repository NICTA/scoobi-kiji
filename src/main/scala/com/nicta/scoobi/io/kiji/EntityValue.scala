package com.nicta.scoobi
package io
package kiji

import org.kiji.schema.{EntityIdFactory, EntityId}
import core.WireFormat
import java.io.{DataInput, DataOutput}
import org.kiji.schema.layout.KijiTableLayout

/**
 * This class represents a single value from a Kiji data row.
 *
 * It has an entity id and a well-typed value
 */
case class EntityValue[T](entityId: EntityId, value: T)

object EntityValue {

  implicit def entityValueHasEntityId[T]: EntityIdComponent[EntityValue[T]] = new EntityIdComponent[EntityValue[T]] {
    def toEntityId(v: EntityValue[T]) = v.entityId
  }

  implicit def entityValueHasWireFormat[T](implicit wfid: WireFormat[EntityId], wf : WireFormat[T]): WireFormat[EntityValue[T]] = new WireFormat[EntityValue[T]] {
    def toWire(x: EntityValue[T], out: DataOutput) {
      wfid.toWire(x.entityId, out)
      wf.toWire(x.value, out)
    }

    def fromWire(in: DataInput) = EntityValue(wfid.fromWire(in), wf.fromWire(in))
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

