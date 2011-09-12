package com.foursquare.meadow

import com.foursquare.meadow.Implicits._
import org.bson.BSONObject
import com.mongodb.{BasicDBObject, DBObject}
import scala.collection.mutable.{MutableList, ListMap}

class LockedRecordException(msg: String) extends RuntimeException(msg)

trait FieldCreationMethods {
  def objectIdField(name: String) = FieldDescriptor(name, ObjectIdSerializer)
  def booleanField(name: String) = FieldDescriptor(name, BooleanSerializer)
  def intField(name: String) = FieldDescriptor(name, IntSerializer)
  def longField(name: String) = FieldDescriptor(name, LongSerializer)
  def doubleField(name: String) = FieldDescriptor(name, DoubleSerializer)
  def stringField(name: String) = FieldDescriptor(name, StringSerializer)
  def dateTimeField(name: String) = FieldDescriptor(name, DateTimeSerializer)
  def listField[T](name: String, elementSerializer: Serializer[T]) = {
    FieldDescriptor[List[T]](name, ListSerializer(elementSerializer))
  }
  def recordField[R <: Record[IdType], IdType](name: String, sch: Schema[R, IdType]) = {
    FieldDescriptor[R](name, RecordSerializer(sch))
  }
}

abstract class BaseRecord

/**
 * A base record type to be instantiated for newly created or loaded records.
 * Subclasses must implement 'id' and 'schema' methods and a protected
 * constructor that passes a BSONObject and Boolean up to this class.
 *
 * The IdType type-parameter dictates the type of the primary key of this
 * record. It may be an ObjectId, a Long, or another Record.
 */
abstract class Record[IdType] extends BaseRecord with FieldCreationMethods {
  def id: IdType
  def schema: BaseSchema
  private var fields: MutableList[ValueContainer[_, _, _, _]] = new MutableList() 
  @volatile private var locked = true 

  def init(src: BSONObject, newRecord: Boolean): Unit = {
    locked = false
    for (vc <- fields) {
      vc.init(src, newRecord)
    }
  }

  def clearForReuse: Unit = {
    fields.foreach(_.clearForReuse)
    locked = true
  }

  def assertNotLocked = {
    if (locked) {
      throw new LockedRecordException("Locked record cannot be accessed for reading or writing!")
    }
  }

  /**
   * Serializes the record into a DBObject.
   */
  def serialize(): DBObject = {
    val res = new BasicDBObject()
    for (vc <- fields) {
      vc.serialize.foreach(serializedField => res.put(vc.descriptor.name, serializedField.v))
    }
    res
  }

  def build[T, Reqd <: MaybeExists, Ext <: Extension[T], RecordType <: Record[_]](
      name: String, fdCreator: String => FieldDescriptor[T, Reqd, Ext], rec: RecordType): ValueContainer[T, RecordType, Reqd, Ext] = {
    assert(this == rec)
    val fd = schema.getOrCreateFD(name, fdCreator)
    val container = fd.create(rec)
    fields += container 
    container
  }
}
