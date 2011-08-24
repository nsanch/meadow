package com.foursquare.meadow

import com.mongodb.DBObject

import com.foursquare.meadow.Implicits._

// These phantom types are used to ensure that a FieldDescriptor doesn't let
// you specify that a field is both required and has a default value, and also
// that the 'get' method is only callable if a field is either required or has
// a default value. For more about phantom types, check out
// http://engineering.foursquare.com/2011/01/31/going-rogue-part-2-phantom-types/
sealed abstract class MaybeExists
sealed abstract class MustExist extends MaybeExists
sealed abstract class NotRequiredToExist extends MaybeExists

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

abstract class BaseValueContainer {
  def isDefined: Boolean
  def descriptor: BaseFieldDescriptor
  def serialize: Option[PhysicalType]
  def clearForReuse: Unit
}
  
/**
 * Contains a mutable value and allows access to an Option, set's, unset's, and
 * tests to see if the value has changed since the container was constructed.
 * Exposes an Extension to provide additional functionality that is unique to
 * the given container.
 */
abstract class ValueContainer[T, +RecordType <: BaseRecord, +Reqd <: MaybeExists, Ext <: Extension[T]] extends BaseValueContainer {
  /**
   * Initializes the container with the given field.
   */
  def init(initFrom: Option[T]): Unit

  /**
   * Returns the underlying value as a Some if it exists. If the value wasn't
   * set and a default value was specified during creation of the
   * ValueContainer, returns that defaultValue. Otherwise, returns None. 
   */
  def getOpt: Option[T]
  
  /**
   * Whether the underlying value is defined. Returns false when the value is
   * undefined, even if a default value is available.
   */
  def isDefined: Boolean
  
  /**
   * Sets the value to t.
   */
  def apply(t: Option[T]): RecordType 
  def apply(t: T): RecordType = apply(Some(t))

  /**
   * Whether the underlying value has changed since the container was created.
   */
  def isDirty: Boolean

  /**
   * Returns optional extensions specific to this container.
   */
  def ext: Ext

  /**
   * Gets the underlying value of this field. If the value is unset and a
   * default value is available, returns that. If the value is unset and the
   * field was simply marked required without a default value, throws an
   * exception.
   *
   * Only required fields can use this method.
   */
  def get[R >: Reqd](implicit ev: R =:= MustExist): T
}

// This is the sole concrete implementation of ValueContainer. It should not be
// extended or directly constructed except by FieldDescriptor.
private[meadow] final class ConcreteValueContainer[T, RecordType <: Record[_], Reqd <: MaybeExists, Ext <: Extension[T]](
    override val descriptor: FieldDescriptor[T, Reqd, Ext],
    owner: RecordType,
    extensionCreator: ValueContainer[T, BaseRecord, MaybeExists, Ext] => Ext,
    behaviorWhenUnset: Option[UnsetBehavior[T]]) extends ValueContainer[T, RecordType, Reqd, Ext] {
  private var _valueOpt: Option[T] = None

  val ext: Ext = extensionCreator(this)
  
  override def clearForReuse: Unit = {
    _valueOpt = None
    ext.clearForReuse
  }

  override def init(initFrom: Option[T]): Unit = {
    _valueOpt = initFrom
    ext.init
  }

  override def apply(newOpt: Option[T]): RecordType = {
    if (newOpt !=? _valueOpt) {
      val oldOpt = _valueOpt
      _valueOpt = newOpt
      owner.onChange(this, oldOpt, newOpt)
      ext.onChange(oldOpt, _valueOpt)
    }
    owner
  }
  
  // Public interface
  override def getOpt: Option[T] = {
    if (_valueOpt.isDefined) {
      _valueOpt
    } else {
      behaviorWhenUnset.flatMap(_.onGetOpt(_valueOpt))
    }
  }
  override def isDirty: Boolean = owner.hasChange(this) 
  override def isDefined: Boolean = _valueOpt.isDefined 

  override def serialize: Option[PhysicalType] = _valueOpt.map(descriptor.serializer.serialize _)

  // Only required fields can use this method.
  override def get[R >: Reqd](implicit ev: R =:= MustExist): T = {
    // behaviorWhenUnset must be defined for any MustExist containers
    behaviorWhenUnset.get.onGet(_valueOpt)
  }
}
