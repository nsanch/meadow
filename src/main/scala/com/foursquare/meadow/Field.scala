package com.foursquare.meadow

import com.mongodb.DBObject

import com.foursquare.meadow.Implicits._

sealed abstract class MaybeRequired
sealed abstract class Required extends MaybeRequired
sealed abstract class NotRequired extends MaybeRequired

class ValueContainer[T, Reqd <: MaybeRequired](initFrom: Option[T],
                                               behaviorWhenUnset: Option[T] => T) {
  private var _origValueOpt: Option[T] = initFrom
  private var _valueOpt: Option[T] = initFrom
  private var _dirty = false

  protected def set(newOpt: Option[T]): Unit = {
    if (newOpt !=? _valueOpt) {
      _valueOpt = newOpt
      _dirty = (_valueOpt !=? _origValueOpt) 
    }
  }
  
  // Public interface
  def getOpt: Option[T] = _valueOpt 
  def set(t: T): Unit = set(Some(t))
  def isDirty: Boolean = _dirty
  def unset: Unit = set(None)
  def isDefined: Boolean = getOpt.isDefined 

  // Only required fields can use this method.
  def get(implicit ev: Reqd =:= Required): T = {
    behaviorWhenUnset(getOpt)
  }
}
