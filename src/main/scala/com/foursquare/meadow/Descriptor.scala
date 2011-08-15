package com.foursquare.meadow

import com.mongodb.{BasicDBObject, DBCollection, DBObject, WriteConcern, BasicDBList}
import org.bson.BSONObject
import org.bson.types.ObjectId
import org.joda.time.DateTime
import scala.collection.mutable.MutableList
import scala.reflect.Manifest

sealed abstract class UnsetBehavior[T] {
  def onGetOpt(opt: Option[T]): Option[T]
  def onGet(opt: Option[T]): T
}
case class AssertingBehavior[T]() extends UnsetBehavior[T] {
  override def onGetOpt(opt: Option[T]): Option[T] = opt
  override def onGet(opt: Option[T]): T = opt.get
}
case class DefaultValueBehavior[T](defaultVal: T) extends UnsetBehavior[T] {
  override def onGetOpt(opt: Option[T]): Option[T] = opt.orElse(Some(defaultVal))
  override def onGet(opt: Option[T]): T = opt.getOrElse(defaultVal)
}

abstract class BaseFieldDescriptor {
  def name: String
}

object FieldDescriptor {
  def extractValue[T](serializer: Serializer[T], src: BSONObject, name: String): Option[T] = {
    if (src.containsField(name)) {
      Some(serializer.deserialize(PhysicalType(src.get(name))))
    } else None
  }

  def apply[T](name: String, serializer: Serializer[T]) = {
    new FieldDescriptor[T, NotRequired, NoExtensions[T]](name,
                                                         serializer,
                                                         _ => NoExtensions())
  }
}

class FieldDescriptor[T, Reqd <: MaybeRequired, Ext <: Extensions[T]](
    override val name: String,
    val serializer: Serializer[T],
    val extensions: ExtendableValueContainer[T, Ext] => Ext,
    val generatorOpt: Option[Generator[T]] = None,
    val behaviorWhenUnset: Option[UnsetBehavior[T]] = None) extends BaseFieldDescriptor {

  def extractFrom(src: BSONObject): ValueContainer[T, Reqd, Ext] = {
    new ConcreteValueContainer(this,
                               FieldDescriptor.extractValue(serializer,
                                                            src,
                                                            name),
                               extensions,
                               behaviorWhenUnset)
  }
  
  def createForNewRecord(): ValueContainer[T, Reqd, Ext] = {
    val vc = new ConcreteValueContainer(this,
                                        None,
                                        extensions,
                                        behaviorWhenUnset)
    // Do this set instead of initializing with this value because that ensures
    // that the value will be marked dirty.
    generatorOpt.map(g => vc.set(g.generate()))
    vc
  }
  
  def required_!()(implicit ev: Reqd =:= NotRequired): FieldDescriptor[T, Required, Ext] = {
    new FieldDescriptor[T, Required, Ext](this.name, this.serializer, this.extensions, this.generatorOpt, Some(AssertingBehavior()))
  }

  def withDefaultValue(defaultVal: T)(implicit ev: Reqd =:= NotRequired): FieldDescriptor[T, Required, Ext] = {
    new FieldDescriptor[T, Required, Ext](this.name, this.serializer, this.extensions, this.generatorOpt, Some(DefaultValueBehavior(defaultVal)))
  }

  def withGenerator(generator: Generator[T]): FieldDescriptor[T, Reqd, Ext] = {
    new FieldDescriptor[T, Reqd, Ext](this.name, this.serializer, this.extensions, Some(generator), this.behaviorWhenUnset)
  }

  def withExtensions[NewExt <: Extensions[T]](extCreator: ExtendableValueContainer[T, NewExt] => NewExt): FieldDescriptor[T, Reqd, NewExt] = {
    new FieldDescriptor[T, Reqd, NewExt](this.name, this.serializer, extCreator, this.generatorOpt, this.behaviorWhenUnset)
  }

// on creation:
// - if unset, set with optional generator. even optional fields can have a generator, for introduction of new fields for example.
// during normal use, if 'get' method is desired:
// - assert existence and throw when not available? _id for example. get = getOpt.get
// - okay with nonexistence and provide a default value to return from get and getOpt
}

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
    for (container <- rec.fields;
         oneField <- container.serialize) {
      res.put(container.descriptor.name, oneField.v) 
    }
    res
  }

  def save(r: Record[_]) = {
    // is it better to do insert for new records? appears to work fine either way
    coll().save(serialize(r), WriteConcern.NORMAL)
  }
}

case class MongoLocation(db: String, collection: String)

abstract class RecordDescriptor[RecordType <: Record[IdType], IdType] extends BaseRecordDescriptor {
  protected def createInstance(dbo: BSONObject, newRecord: Boolean): RecordType
  final def createRecord: RecordType = createInstance(new BasicDBObject(), true)
  final def loadRecord(dbo: BSONObject): RecordType = createInstance(dbo, false)
    
  def findOne(id: IdType): Option[RecordType] = findAll(List(id)).headOption

  def findAll(ids: List[IdType]): List[RecordType] = {
    if (ids.isEmpty) {
      Nil
    } else {
      val idList = new BasicDBList()
      ids.foreach(id => idList.add(id.asInstanceOf[AnyRef]))
      val found = coll().find(new BasicDBObject("_id", new BasicDBObject("$in", idList))) 
      val l = new MutableList[RecordType]()
      while (found.hasNext()) {
        l += loadRecord(found.next())
      }
      l.toList
    }
  }
}

abstract class Record[IdType](dbo: BSONObject, newRecord: Boolean) {
  def id: IdType
  def descriptor: BaseRecordDescriptor
  var fields: MutableList[BaseValueContainer] = new MutableList()

  def build[T, Reqd <: MaybeRequired, Ext <: Extensions[T]](fd: FieldDescriptor[T, Reqd, Ext]): ValueContainer[T, Reqd, Ext] = {
    val container = (if (newRecord) {
      fd.createForNewRecord()
    } else {
      fd.extractFrom(dbo)
    })
    fields += container
    container
  }

  def save = {
    descriptor.save(this)
  }
}
