package com.foursquare.meadow

import com.mongodb.{BasicDBObject, DBCollection, DBObject, WriteConcern, BasicDBList}
import org.bson.BSONObject
import org.bson.types.ObjectId
import org.joda.time.DateTime
import scala.collection.mutable.{ListBuffer, ListMap}
import scala.reflect.Manifest

// These phantom types are used to ensure that a FieldDescriptor is never given
// two extensions. For more about phantom types, check out
// http://engineering.foursquare.com/2011/01/31/going-rogue-part-2-phantom-types/
sealed abstract class MaybeExtended
sealed abstract class Extended extends MaybeExtended
sealed abstract class NotExtended extends MaybeExtended

// These classes encapsulate two different ways that a ValueContainer may
// handle calls to 'get' and 'getOpt' when a value does not exist.
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
  /**
   * Helper method used to create field descriptors that start out optional and
   * unextended, but have a type and a serializer.
   */
  def apply[T](name: String, serializer: Serializer[T]) = {
    new FieldDescriptor[T, NotRequiredToExist, NoExtensions[T], NotExtended](name,
                                                                             serializer,
                                                                             _ => NoExtensions())
  }
}

/**
 * This class acts as a builder for ValueContainer's and can be combined with
 * RecordDescriptor to form a schema for a particular collection.
 */
class FieldDescriptor[T, Reqd <: MaybeExists, Ext <: Extensions[T], Extd <: MaybeExtended](
    override val name: String,
    val serializer: Serializer[T],
    val extensions: ExtendableValueContainer[T, Ext] => Ext,
    val generatorOpt: Option[Generator[T]] = None,
    val behaviorWhenUnset: Option[UnsetBehavior[T]] = None) extends BaseFieldDescriptor {

  /**
   * Used when a ValueContainer is built from this FieldDescriptor, given a
   * BSONObject that may or may not have a key for this data under
   * <code>name</code>.
   */
  def extractFrom(src: BSONObject): ValueContainer[T, Reqd, Ext] = {
    val value = (
      if (src.containsField(name)) {
        Some(serializer.deserialize(PhysicalType(src.get(name))))
      } else None
    )
    new ConcreteValueContainer(this,
                               value,
                               extensions,
                               behaviorWhenUnset)
  }

  /**
   * Used when a ValueContainer is created for a new record. If a generator
   * exists, this calls it to set a value.
   */
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
  
  /**
   * Required fields have a callable <code>get</code> method that asserts that
   * the value is defined. getOpt will not do any such assertion.
   *
   * Fields may not be marked required and also have a default value. If the
   * default value would ever be used, then the field is by definition not
   * required, or was not at some point in the past. It is only safe to mark a
   * field required if all existing records and all new records will have the
   * value set.
   */
  def required_!()(implicit ev: Reqd =:= NotRequiredToExist): FieldDescriptor[T, MustExist, Ext, Extd] = {
    new FieldDescriptor[T, MustExist, Ext, Extd](this.name, this.serializer, this.extensions, this.generatorOpt, Some(AssertingBehavior()))
  }

  /**
   * Fields with a default value will have a callable <code>get</code> method
   * that returns the default value if the value is not explicitly defined.
   *
   * Fields may not be marked required and also have a default value. If the
   * default value would ever be used, then the field is by definition not
   * required, or was not at some point in the past.
   */
  def withDefaultValue(defaultVal: T)(implicit ev: Reqd =:= NotRequiredToExist): FieldDescriptor[T, MustExist, Ext, Extd] = {
    new FieldDescriptor[T, MustExist, Ext, Extd](this.name, this.serializer, this.extensions, this.generatorOpt, Some(DefaultValueBehavior(defaultVal)))
  }

  /**
   * If a generator is specified, then, on record-creation, all generated
   * fields will be given an initial value as specified by their generator.
   *
   * Having a generator does not automatically make a field required because
   * there may be existing fields in the DB that do not have the value set.
   */
  def withGenerator(generator: Generator[T]): FieldDescriptor[T, Reqd, Ext, Extd] = {
    new FieldDescriptor[T, Reqd, Ext, Extd](this.name, this.serializer, this.extensions, Some(generator), this.behaviorWhenUnset)
  }

  /**
   * Adds a typed extension to the ValueContainer.
   *
   * It is not possible to specify two extensions on one FieldDescriptor or
   * ValueContainer.
   */
  def withExtensions[NewExt <: Extensions[T]](extCreator: ExtendableValueContainer[T, NewExt] => NewExt)
                                             (implicit ev: Extd =:= NotExtended): FieldDescriptor[T, Reqd, NewExt, Extended] = {
    new FieldDescriptor[T, Reqd, NewExt, Extended](this.name, this.serializer, extCreator, this.generatorOpt, this.behaviorWhenUnset)
  }

  /**
   * A helper method to specify that a field should use a foreign key extension
   * pointing at the given RecordDescriptor.
   */
  def withFKExtensions[RecordType <: Record[T]](desc: RecordDescriptor[RecordType, T])
                                               (implicit ev: Extd =:= NotExtended): FieldDescriptor[T, Reqd, FKExtension[RecordType, T], Extended] = {
    withExtensions[FKExtension[RecordType, T]](vc => new FKExtension(vc, desc))
  }
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

  def build[T, Reqd <: MaybeExists, Ext <: Extensions[T]](fd: FieldDescriptor[T, Reqd, Ext, _]): ValueContainer[T, Reqd, Ext] = {
    val container = (if (newRecord) {
      fd.createForNewRecord()
    } else {
      fd.extractFrom(dbo)
    })
    fields += Tuple2(fd: BaseFieldDescriptor, container)
    container
  }

  def save = synchronized {
    descriptor.save(this)
  }
}
