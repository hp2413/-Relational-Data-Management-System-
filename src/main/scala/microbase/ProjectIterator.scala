package microbase

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, EmptyRow, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

class ProjectIterator (child: Iterator[InternalRow], boundCondition: Seq[Expression] ) extends Iterator[InternalRow] {

  override def hasNext: Boolean = {
    //println("do Project has next? "+ child.hasNext)
    child.hasNext
  }

  override def next(): InternalRow ={
    val newChild = child.next()
    if(newChild != null) {
      var newRow: Seq[Any] = Seq[Any]()
      //println("Project: " + boundCondition)
      val result = boundCondition.map(expr => {
        newRow = newRow :+ expr.eval(newChild)
//        expr match {
//          case boundReference: BoundReference => {
//            newRow = newRow :+ newChild.get(boundReference.ordinal, boundReference.dataType)
//          }
//          case expression: Expression => {
//            newRow = newRow :+ expr.eval(newChild)
//          }
//        }
      })
      val newIntr: InternalRow = InternalRow.fromSeq(newRow)
//      println("Result"+ newIntr)
      newIntr
    }
    else{
      null
    }
  }

}