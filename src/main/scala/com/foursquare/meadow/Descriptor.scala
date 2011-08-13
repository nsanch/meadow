package com.foursquare.meadow

import com.mongodb.BasicDBObject
import org.bson.BSONObject
import org.bson.types.ObjectId
import org.joda.time.DateTime
import scala.reflect.Manifest

abstract class BaseFieldDescriptor {
  def name: String
  def serialize(container: BaseValueContainer): Option[Any]
}

object FieldDescriptor {
  def extractValue[T](serializer: Serializer[T], src: BSONObject, name: String): Option[T] = {
    if (src.containsField(name)) {
      serializer.deserialize(src.get(name))
    } else None
  }
}

class FieldDescriptor[T, Reqd <: MaybeRequired](
    override val name: String,
    val serializer: Serializer[T],
    val generatorOpt: Option[Generator[T]] = None,
    val behaviorWhenUnset: Option[Option[T] => T] = None) extends BaseFieldDescriptor {

  def extractFrom(src: BSONObject): ValueContainer[T, Reqd] = {
    new ConcreteValueContainer(this,
                               FieldDescriptor.extractValue(serializer,
                                                            src,
                                                            name),
                               behaviorWhenUnset)
  }
  
  def createForNewRecord(): ValueContainer[T, Reqd] = {
    new ConcreteValueContainer(this,
                               generatorOpt.map(_.generate()),
                               behaviorWhenUnset)
  }
  
  override def serialize(container: BaseValueContainer): Option[Any] = container match {
    // TODO(nsanch): due to type erasure this'll match anything
    case vc: ValueContainer[T, Reqd] =>
      if (vc.isDefined) {
        vc.getOpt.map(serializer.serialize _)
      } else None
    case _ => None
  }

  def required()(implicit ev: Reqd =:= NotRequired): FieldDescriptor[T, Required] = {
    new FieldDescriptor[T, Required](this.name, this.serializer, this.generatorOpt, Some(_.get))
  }

  def withDefaultValue(defaultVal: T)(implicit ev: Reqd =:= NotRequired): FieldDescriptor[T, Required] = {
    new FieldDescriptor[T, Required](this.name, this.serializer, this.generatorOpt, Some(_.getOrElse(defaultVal)))
  }

  def withGenerator(generator: Generator[T]): FieldDescriptor[T, Reqd] = {
    new FieldDescriptor[T, Reqd](this.name, this.serializer, Some(generator), this.behaviorWhenUnset)
  }

// on creation:
// - if unset, set with optional generator. even optional fields can have a generator, for introduction of new fields for example.
// during normal use, if 'get' method is desired:
// - assert existence and throw when not available? _id for example. get = getOpt.get
// - okay with nonexistence and provide a default value to return from get and getOpt
}

abstract class RecordDescriptor[RecordType <: Record] {
  protected def createInstance(dbo: BSONObject, newRecord: Boolean): RecordType

  final def createRecord: RecordType = createInstance(new BasicDBObject(), true)
  final def loadRecord(dbo: BSONObject): RecordType = {
    createInstance(dbo, false)
  }
    
  def serialize(rec: RecordType): BSONObject = {
    val res = new BasicDBObject()
    for (container <- rec.fields;
         oneField <- container.descriptor.serialize(container)) {
      res.put(container.descriptor.name, oneField) 
    }
    res
  }

  def objectIdField(name: String) = new FieldDescriptor[ObjectId, NotRequired](name, ObjectIdSerializer)
  def longField(name: String) = new FieldDescriptor[Long, NotRequired](name, LongSerializer)
  def intField(name: String) = new FieldDescriptor[Int, NotRequired](name, IntSerializer)
  def stringField(name: String) = new FieldDescriptor[String, NotRequired](name, StringSerializer)
  def dateTimeField(name: String) = new FieldDescriptor[DateTime, NotRequired](name, DateTimeSerializer)
  def listField[T](name: String, elementSerializer: Serializer[T]) = {
    new FieldDescriptor[List[T], NotRequired](name, ListSerializer(elementSerializer))
  }
  def recordField[R <: Record](name: String, desc: RecordDescriptor[R]) = {
    new FieldDescriptor[R, NotRequired](name, RecordSerializer(desc))
  }
}

abstract class Record(dbo: BSONObject, newRecord: Boolean) {
  var fields: List[BaseValueContainer] = Nil

  def field[T, Reqd <: MaybeRequired](fd: FieldDescriptor[T, Reqd]): ValueContainer[T, Reqd] = {
    val container = (if (newRecord) {
      fd.createForNewRecord()
    } else {
      fd.extractFrom(dbo)
    })
    fields = container :: fields
    container
  }
}
