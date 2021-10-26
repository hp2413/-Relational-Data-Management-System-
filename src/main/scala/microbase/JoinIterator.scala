package microbase

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow

class JoinIterator (leftChild: Iterator[InternalRow] , rightChild: Iterator[InternalRow] ) extends Iterator[InternalRow] {

  var leftTableROW : InternalRow = null
  var rightRows = rightChild.toSeq
  var count = 0

  override def hasNext: Boolean = {
    // println("do Join has next?")
    //    System.err.println("Left Child HasNext "+leftChild.hasNext)
    //    System.err.println("Right Child HasNext "+rightChild.hasNext)
    //    System.err.println("BC from Join from HashNext "+boundJoinCondition)
    if(leftChild.hasNext){
      if(count < rightRows.length){
        true
      }else{
        count = 0
        leftTableROW = null
        true
      }
    }else{
      if(count < rightRows.length){
        true
      }else{
        count = 0
        leftTableROW = null
        false
      }
    }
  }

  override def next(): InternalRow = {
    if (leftTableROW == null) {
      leftTableROW = leftChild.next()
      callCombineChild()
    }
    else{
      callCombineChild()
    }
  }

  def callCombineChild() : InternalRow={
    var newRows: InternalRow = null
    val rightTableROW = rightRows(count)
    count = count + 1
    val combinedROW = new JoinedRow(leftTableROW, rightTableROW)
    if (combinedROW != null && leftTableROW != null && rightTableROW != null ){
      newRows = combinedROW
    }
    newRows
  }

}
