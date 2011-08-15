package com.foursquare.meadow

import org.bson.BSONObject
import com.mongodb.{BasicDBObject, DBCollection, DBObject, WriteConcern, BasicDBList}
import org.bson.types.ObjectId
import org.joda.time.DateTime
import scala.collection.mutable.{ListBuffer, ListMap}

abstract class BaseRecordDescriptor {
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
  def recordField[R <: Record[IdType], IdType](name: String, desc: RecordDescriptor[R, IdType]) = {
    FieldDescriptor[R](name, RecordSerializer(desc))
  }

  protected def mongoLocation: MongoLocation

  // TODO(nsanch): As much as possible, the mongo logic should move to another class.
  def coll(): DBCollection = MongoConnector.mongo.getDB(mongoLocation.db).getCollection(mongoLocation.collection)

  def serialize(rec: Record[_]): DBObject = {
    val res = new BasicDBObject()
    for ((fieldDesc, container) <- rec.fields;
         serializedField <- container.serialize) {
      res.put(fieldDesc.name, serializedField.v) 
    }
    res
  }

  def save(r: Record[_]) = {
    // TODO(nsanch): is it better to do insert for new records? appears to work
    // fine either way.
    coll().save(serialize(r), WriteConcern.NORMAL)
  }
}

case class MongoLocation(db: String, collection: String)

abstract class RecordDescriptor[RecordType <: Record[IdType], IdType] extends BaseRecordDescriptor {
  protected def createInstance(dbo: BSONObject, newRecord: Boolean): RecordType
  final def createRecord: RecordType = createInstance(new BasicDBObject(), true)
  final def loadRecord(dbo: BSONObject): RecordType = createInstance(dbo, false)
    
  def findOne(id: IdType): Option[RecordType] = findAll(List(id)).headOption

  def findAll(ids: Traversable[IdType]): List[RecordType] = {
    if (ids.isEmpty) {
      Nil
    } else {
      val idList = new BasicDBList()
      ids.foreach(id => idList.add(id.asInstanceOf[AnyRef]))
      val found = coll().find(new BasicDBObject("_id", new BasicDBObject("$in", idList))) 
      val l = new ListBuffer[RecordType]()
      l.sizeHint(ids)
      while (found.hasNext()) {
        l += loadRecord(found.next())
      }
      l.result()
    }
  }
    
  def prime[ContainingRecord <: Record[_],
            Ext <: Extensions[IdType] with ForeignKeyLogic[RecordType, IdType]](
      containingRecords: List[ContainingRecord],
      lambda: ContainingRecord => ExtendableValueContainer[IdType, Ext],
      known: List[RecordType] = Nil): List[ContainingRecord] = {
    PrimingLogic.prime(this, containingRecords, lambda, known)
  }
}

abstract class Record[IdType](dbo: BSONObject, newRecord: Boolean) {
  def id: IdType
  def descriptor: BaseRecordDescriptor
  var fields: ListMap[BaseFieldDescriptor, BaseValueContainer] = new ListMap()

  def build[T, Reqd <: MaybeExists, Ext <: Extensions[T]](fd: FieldDescriptor[T, Reqd, Ext]): ValueContainer[T, Reqd, Ext] = {
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
    descriptor.save(this)
  }
}
