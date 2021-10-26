package microbase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression}
import org.apache.spark.sql.types.DataType

import java.time.LocalDate
import scala.collection.mutable


class FilterIterator (boundCondition : Expression, child: Iterator[InternalRow]) extends Iterator[InternalRow]{
  override def hasNext: Boolean = {
    filterItr.hasNext
  }


  var filterItr = child.filter({ row =>
    boundCondition.eval(row).asInstanceOf[Boolean] })

  override def next(): InternalRow = {
    val newValue = filterItr.next()
    newValue
  }
}


