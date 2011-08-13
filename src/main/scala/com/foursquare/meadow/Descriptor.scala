package com.foursquare.meadow

import com.mongodb.{BasicDBObject, DBObject}
import org.bson.types.ObjectId
import scala.reflect.Manifest

abstract class BaseDescriptor

object FieldDescriptor {
  def extractValue[T](serializer: Serializer[T], src: DBObject, name: String): Option[T] = {
    if (src.containsField(name)) {
      serializer.deserialize(src.get(name))
    } else None
  }
}

class FieldDescriptor[T, Reqd <: MaybeRequired](
    val name: String,
    val serializer: Serializer[T],
    val generatorOpt: Option[Generator[T]] = None,
    val behaviorWhenUnset: Option[T] => T = ((x: Option[T]) => x.get)) extends BaseDescriptor {

  def extractFrom(src: DBObject): ValueContainer[T, Reqd] = {
    new ValueContainer[T, Reqd](FieldDescriptor.extractValue(serializer,
                                                             src,
                                                             name),
                                behaviorWhenUnset)
  }
  
  def createForNewRecord(): ValueContainer[T, Reqd] = {
    new ValueContainer[T, Reqd](generatorOpt.map(_.generate()),
                                behaviorWhenUnset)
  }

  def required(): FieldDescriptor[T, Required] = {
    new FieldDescriptor[T, Required](this.name, this.serializer, this.generatorOpt, this.behaviorWhenUnset) 
  }

  def withDefaultValue(defaultVal: T): FieldDescriptor[T, Required] = {
    new FieldDescriptor[T, Required](this.name, this.serializer, this.generatorOpt, _.getOrElse(defaultVal))
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

abstract class RecordDescriptor[RecordType] {
  protected def createInstance(dbo: DBObject, newRecord: Boolean): RecordType

  final def createRecord: RecordType = createInstance(new BasicDBObject(), true)
  final def loadRecord(dbo: DBObject): RecordType = {
    createInstance(dbo, false)
  }

  def objectIdField(name: String) = new FieldDescriptor[ObjectId, NotRequired](name, ObjectIdSerializer)
  def longField(name: String) = new FieldDescriptor[Long, NotRequired](name, LongSerializer)
  def intField(name: String) = new FieldDescriptor[Int, NotRequired](name, IntSerializer)
  def stringField(name: String) = new FieldDescriptor[String, NotRequired](name, StringSerializer)
}

abstract class Record(dbo: DBObject, newRecord: Boolean) {
  def field[T, Reqd <: MaybeRequired](fd: FieldDescriptor[T, Reqd]): ValueContainer[T, Reqd] = {
    if (newRecord) {
      fd.extractFrom(dbo)
    } else {
      fd.createForNewRecord()
    }
  }
}


//new ObjectIdField with RequiredField
//new StringField with DefaultValuedField { def defaultVal = "" }
