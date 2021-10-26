package microbase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference}
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

trait GeneralIterator extends Iterator[InternalRow]{
  def getMyPostions():  mutable.HashMap[Attribute,BoundReference]
  override def hasNext: Boolean
  override def next(): InternalRow
  def reset() : Unit
}
