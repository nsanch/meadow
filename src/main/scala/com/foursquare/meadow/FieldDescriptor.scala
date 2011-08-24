package com.foursquare.meadow

import org.bson.BSONObject

// These phantom types are used to ensure that a FieldDescriptor is never given
// two extensions. For more about phantom types, check out
// http://engineering.foursquare.com/2011/01/31/going-rogue-part-2-phantom-types/
sealed abstract class MaybeExtended
sealed abstract class Extended extends MaybeExtended
sealed abstract class NotExtended extends MaybeExtended

abstract class BaseFieldDescriptor {
  def name: String
}

object FieldDescriptor {
  /**
   * Helper method used to create field descriptors that start out optional and
   * unextended, but have a type and a serializer.
   */
  def apply[T](name: String, serializer: Serializer[T]) = {
    new FieldDescriptor[T, NotRequiredToExist, NoExtensions[T]](name,
                                                                serializer,
                                                                _ => NoExtensions())
  }
}

/**
 * This class acts as a builder for ValueContainer's and can be combined with
 * Schema to form a schema for a particular collection.
 */
class FieldDescriptor[T, Reqd <: MaybeExists, Ext <: Extension[T]](
    override val name: String,
    val serializer: Serializer[T],
    val extensions: ValueContainer[T, BaseRecord, MaybeExists, Ext] => Ext,
    val generatorOpt: Option[Generator[T]] = None,
    val behaviorWhenUnset: Option[UnsetBehavior[T]] = None) extends BaseFieldDescriptor {

  /**
   * Used when a ValueContainer is built from this FieldDescriptor.
   */
  def create[RecordType <: BaseRecord, IdType](owner: RecordType): ValueContainer[T, RecordType, Reqd, Ext] = {
    new ConcreteValueContainer(this,
                               owner,
                               extensions,
                               behaviorWhenUnset)
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
  def required_!()(implicit ev: Reqd =:= NotRequiredToExist): FieldDescriptor[T, MustExist, Ext] = {
    new FieldDescriptor[T, MustExist, Ext](this.name, this.serializer, this.extensions, this.generatorOpt, Some(AssertingBehavior()))
  }

  /**
   * Fields with a default value will have a callable <code>get</code> method
   * that returns the default value if the value is not explicitly defined.
   *
   * Fields may not be marked required and also have a default value. If the
   * default value would ever be used, then the field is by definition not
   * required, or was not at some point in the past.
   */
  def withDefaultValue(defaultVal: T)(implicit ev: Reqd =:= NotRequiredToExist): FieldDescriptor[T, MustExist, Ext] = {
    new FieldDescriptor[T, MustExist, Ext](this.name, this.serializer, this.extensions, this.generatorOpt, Some(DefaultValueBehavior(defaultVal)))
  }

  /**
   * If a generator is specified, then, on record-creation, all generated
   * fields will be given an initial value as specified by their generator.
   *
   * Having a generator does not automatically make a field required because
   * there may be existing fields in the DB that do not have the value set.
   */
  def withGenerator(generator: Generator[T]): FieldDescriptor[T, Reqd, Ext] = {
    new FieldDescriptor[T, Reqd, Ext](this.name, this.serializer, this.extensions, Some(generator), this.behaviorWhenUnset)
  }

  /**
   * Adds a typed extension to the ValueContainer.
   *
   * It is not possible to specify two extensions on one FieldDescriptor or
   * ValueContainer. Extensions are also not given any information about
   * whether the ValueContainer is required, so they may not use its 'get'
   * method.
   */
  def withExtensions[NewExt <: Extension[T]](extCreator: ValueContainer[T, BaseRecord, MaybeExists, NewExt] => NewExt)
                                             (implicit ev: Ext =:= NoExtensions[T]): FieldDescriptor[T, Reqd, NewExt] = {
    new FieldDescriptor[T, Reqd, NewExt](this.name, this.serializer, extCreator, this.generatorOpt, this.behaviorWhenUnset)
  }

  /**
   * A helper method to specify that a field should use a foreign key extension
   * pointing at the given Schema.
   */
  def withFKExtensions[RefRecordType <: Record[T]](desc: Schema[RefRecordType, T])
                                                  (implicit ev: Ext =:= NoExtensions[T]): FieldDescriptor[T, Reqd, FKExtension[RefRecordType, T]] = {
    withExtensions[FKExtension[RefRecordType, T]](vc => new FKExtension(vc, desc))
  }
}
