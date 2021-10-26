package microbase

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression}
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

class LimitIterator(child: Iterator[InternalRow], boundCondition: Expression ) extends Iterator[InternalRow] {

  override def hasNext: Boolean = {
    if (myCounter == boundCondition.eval(null).asInstanceOf[Int]){
      false
    }else{
      myCounter = myCounter + 1
      child.hasNext
    }
  }

  var myCounter : Int = 0

  override def next(): InternalRow = {
    child.next()
  }


}


