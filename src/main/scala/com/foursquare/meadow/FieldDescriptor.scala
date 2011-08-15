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
 * RecordDescriptor to form a schema for a particular collection.
 */
class FieldDescriptor[T, Reqd <: MaybeExists, Ext <: Extensions[T]](
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
   * ValueContainer.
   */
  def withExtensions[NewExt <: Extensions[T]](extCreator: ExtendableValueContainer[T, NewExt] => NewExt)
                                             (implicit ev: Ext =:= NoExtensions[T]): FieldDescriptor[T, Reqd, NewExt] = {
    new FieldDescriptor[T, Reqd, NewExt](this.name, this.serializer, extCreator, this.generatorOpt, this.behaviorWhenUnset)
  }

  /**
   * A helper method to specify that a field should use a foreign key extension
   * pointing at the given RecordDescriptor.
   */
  def withFKExtensions[RecordType <: Record[T]](desc: RecordDescriptor[RecordType, T])
                                               (implicit ev: Ext =:= NoExtensions[T]): FieldDescriptor[T, Reqd, FKExtension[RecordType, T]] = {
    withExtensions[FKExtension[RecordType, T]](vc => new FKExtension(vc, desc))
  }
}
