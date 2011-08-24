package com.foursquare.meadow

import com.foursquare.meadow.Implicits._
import org.bson.BSONObject
import com.mongodb.{BasicDBObject, DBObject}
import scala.collection.mutable.{MutableList, ListMap}

case class DescriptorValuePair[T, Reqd <: MaybeExists, Ext <: Extension[T]](
    fd: FieldDescriptor[T, Reqd, Ext],
    vc: ValueContainer[T, BaseRecord, Reqd, Ext]) {
  def init(src: BSONObject, newRecord: Boolean): Unit = {
    if (!newRecord) {
      if (src.containsField(fd.name)) {
        vc.init(Some(fd.serializer.deserialize(PhysicalType(src.get(fd.name)))))
      } else { 
        vc.init(None)
      }
    } else {
      vc.init(None)
      fd.generatorOpt.map(g => vc(g.generate()))
    }
  }

  def serializeInto(dbo: BasicDBObject): Unit = {
    vc.serialize.foreach(serializedField => dbo.put(fd.name, serializedField.v))
  }
}      

case class Delta[+T](oldOpt: Option[T], newOpt: Option[T])

class ChangeLog {
  private var changes = new ListMap[Any, Delta[Any]]

  def addChange[T](key: Any, change: Delta[T]) = synchronized {
    val oldDeltaOpt = changes.get(key)
    oldDeltaOpt.map(oldDelta => {
      if (oldDelta.oldOpt =? change.newOpt) {
        changes.remove(key)
      } else {   
        changes.put(key, Delta[T](oldDelta.oldOpt.asInstanceOf[Option[T]], change.newOpt))
      }
    }).getOrElse(changes.put(key, change))
  }
  def hasChange(key: Any) = synchronized { changes.contains(key) }
}

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
  private var fields: MutableList[DescriptorValuePair[_, _, _]] = new MutableList() 
  private var changelog: Option[ChangeLog] = None 

  def onChange[T](vc: ValueContainer[T, _, _, _], oldOpt: Option[T], newOpt: Option[T]) = synchronized {
    if (!changelog.isDefined) {
      changelog = Some(new ChangeLog())
    }
    changelog.foreach(_.addChange(vc, Delta[T](oldOpt, newOpt)))
  }
  def hasChange(vc: ValueContainer[_, _, _, _]) = changelog.exists(_.hasChange(vc))

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

  def build[T, Reqd <: MaybeExists, Ext <: Extension[T], RecordType <: Record[_]](
      name: String, fdCreator: String => FieldDescriptor[T, Reqd, Ext], rec: RecordType): ValueContainer[T, RecordType, Reqd, Ext] = {
    assert(this == rec)
    val fd = schema.getOrCreateFD(name, fdCreator)
    val container = fd.create(rec)
    fields += DescriptorValuePair[T, Reqd, Ext](fd, container)
    container
  }
}
