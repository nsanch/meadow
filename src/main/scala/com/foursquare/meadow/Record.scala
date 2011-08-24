package com.foursquare.meadow

import org.bson.BSONObject
import com.mongodb.{BasicDBObject, DBObject}
import scala.collection.mutable.MutableList

case class DescriptorValuePair[T, Reqd <: MaybeExists, Ext <: Extension[T]](fd: FieldDescriptor[T, Reqd, Ext], vc: ValueContainer[T, Reqd, Ext]) {
  def init(src: BSONObject, newRecord: Boolean): Unit = {
    if (!newRecord) {
      if (src.containsField(fd.name)) {
        vc.init(Some(fd.serializer.deserialize(PhysicalType(src.get(fd.name)))))
      } else { 
        vc.init(None)
      }
    } else {
      vc.init(None)
      fd.generatorOpt.map(g => vc.set(g.generate()))
    }
  }

  def serializeInto(dbo: BasicDBObject): Unit = {
    vc.serialize.foreach(serializedField => dbo.put(fd.name, serializedField.v))
  }
}

/**
 * A base record type to be instantiated for newly created or loaded records.
 * Subclasses must implement 'id' and 'schema' methods and a protected
 * constructor that passes a BSONObject and Boolean up to this class.
 *
 * The IdType type-parameter dictates the type of the primary key of this
 * record. It may be an ObjectId, a Long, or another Record.
 */
abstract class Record[IdType] {
  def id: IdType
  def schema: BaseSchema
  private var fields: MutableList[DescriptorValuePair[_, _, _]] = new MutableList() 

  def init(src: BSONObject, newRecord: Boolean): Unit = {
    for (pair <- fields) {
      pair.init(src, newRecord)
    }
  }

  def clearForReuse: Unit = {
    fields.foreach(pair => pair.vc.clearForReuse)
  }

  /**
   * Serializes the record into a DBObject.
   */
  def serialize(): DBObject = {
    val res = new BasicDBObject()
    for (pair <- fields) {
      pair.serializeInto(res)
    }
    res
  }

  def build[T, Reqd <: MaybeExists, Ext <: Extension[T]](fd: FieldDescriptor[T, Reqd, Ext]): ValueContainer[T, Reqd, Ext] = {
    val container = fd.create()
    fields += DescriptorValuePair[T, Reqd, Ext](fd, container)
    container
  }

  def save = synchronized {
    schema.save(this)
  }
}
