package com.foursquare.meadow

import org.bson.BSONObject
import com.mongodb.{BasicDBObject, DBObject}
import scala.collection.mutable.ListMap

/**
 * A base record type to be instantiated for newly created or loaded records.
 * Subclasses must implement 'id' and 'schema' methods and a protected
 * constructor that passes a BSONObject and Boolean up to this class.
 *
 * The IdType type-parameter dictates the type of the primary key of this
 * record. It may be an ObjectId, a Long, or another Record.
 */
abstract class Record[IdType](dbo: BSONObject, newRecord: Boolean) {
  def id: IdType
  def schema: BaseSchema
  private var fields: ListMap[BaseFieldDescriptor, BaseValueContainer] = new ListMap()

  /**
   * Serializes the record into a DBObject.
   */
  def serialize(): DBObject = {
    val res = new BasicDBObject()
    for ((fieldDesc, container) <- fields;
         serializedField <- container.serialize) {
      res.put(fieldDesc.name, serializedField.v) 
    }
    res
  }

  def build[T, Reqd <: MaybeExists, Ext <: Extension[T]](fd: FieldDescriptor[T, Reqd, Ext]): ValueContainer[T, Reqd, Ext] = {
    val container = (
      if (newRecord) {
        fd.createForNewRecord()
      } else {
        fd.extractFrom(dbo)
      }
    )
    fields += Tuple2(fd, container)
    container
  }

  def save = synchronized {
    schema.save(this)
  }
}
