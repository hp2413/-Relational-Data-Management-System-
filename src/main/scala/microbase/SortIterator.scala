package microbase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ Expression, SortOrder}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class SortIterator(condition: Seq[SortOrder], child: Iterator[InternalRow], boundCondition: Seq[Expression] ) extends Iterator[InternalRow] {

  var AllUnSortedList: Seq[InternalRow] = Seq[InternalRow]()
  var sortedRow: Seq[InternalRow] = getMySortedList()
  var index: Integer = 0

  override def hasNext: Boolean = {
    if(index < sortedRow.length){
      true
    }else{
//      println("from else :"+ index )
//      println("from else "+child.hasNext)
//      child.hasNext
      false
    }
  }

  override def next(): InternalRow = {
//    println("Final o/p After sort:"+ index)
//    println(" "+child.hasNext)
    val row = sortedRow(index)
    index += 1
    row
  }

  def getMySortedList() : Seq[InternalRow] = {
    sortedRow = Seq.empty
    AllUnSortedList = child.toSeq.filter(_ != null)
    //println(" Before Sort O/P  " + AllUnSortedList)
    val newValue =  AllUnSortedList.sortWith((x, y) => {  compareTwoInterRows (x = x,y = y, qualifierCount = 0)})
    //println(" Final After Sort O/P  " + newValue)
    newValue
  }

  def compareTwoInterRows( x : InternalRow, y: InternalRow, qualifierCount : Int): Boolean ={
    //System.err.println("called from compare tw0 x "+ x)
    //System.err.println("called from compare tw0 y "+ y)
    val ans = doMyCompare(expression = boundCondition(qualifierCount), x = x, y = y)
    //System.err.println("Ans : "+ ans)
    if(ans > 0){
      if (condition(qualifierCount).direction.toString.equals("Ascending")) {
        false
      }else{
        true
      }
    }
    else if (ans < 0){
      if (condition(qualifierCount).direction.toString.equals("Ascending")) {
        true
      }else{
        false
      }
    }
    else{
      if(qualifierCount < boundCondition.length - 1){
        compareTwoInterRows(x = x, y = y, qualifierCount = qualifierCount + 1)
      }
      else{
        true
      }
    }
  }

  def doMyCompare( expression: Expression, x : InternalRow , y: InternalRow) : Int ={
    val dataType = expression.dataType
    dataType match {
      case IntegerType =>
        expression.eval(x).asInstanceOf[Int].compareTo(expression.eval(y).asInstanceOf[Int])
      case StringType =>
        expression.eval(x).asInstanceOf[UTF8String].toUpperCase.compareTo(expression.eval(y).asInstanceOf[UTF8String].toUpperCase)
      case FloatType =>
        expression.eval(x).asInstanceOf[Float].compareTo(expression.eval(y).asInstanceOf[Float])
      case DoubleType =>
        expression.eval(x).asInstanceOf[Double].compareTo(expression.eval(y).asInstanceOf[Double])
      case LongType =>
        expression.eval(x).asInstanceOf[Long].compareTo(expression.eval(y).asInstanceOf[Long])
      case DateType =>
        expression.eval(x).asInstanceOf[Int].compareTo(expression.eval(y).asInstanceOf[Int])
    }
  }
}