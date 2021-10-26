package microbase
import microbase.Microbase.{bindReferences, splitAnds, splitEqual, AllHashMaps}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

class HashJoinIterator (boundJoinCondition: Expression, condition: Expression, leftChild: Iterator[InternalRow], rightChild: Iterator[InternalRow], allLeft : Seq[Attribute], allRights : Seq[Attribute] ) extends Iterator[InternalRow] {

  val resultExp = allBoundCondition()
  val LeftBoundCondition: Expression = bindReferences(resultExp(0), allLeft)
  val rightBoundCondition: Expression = bindReferences(resultExp(1), allRights)
  var hashJoin: Map[Int, Seq[InternalRow]] = performHash()
  var myBufferSet: Seq[InternalRow] = Seq[InternalRow]()
  var generalCounter: Int = 0

  def allBoundCondition(): Seq[Expression] = {
    var resultExp: Seq[Expression] = Seq[Expression]()
    var myLeft : Expression = null
    var myRight : Expression = null
    val allConditions: Seq[Expression] = splitAnds(condition)
    if (allConditions.nonEmpty) {
      allConditions.map(exp => {
        val individualCondition = splitEqual(exp)
        if (individualCondition.nonEmpty) {
          individualCondition(0).transform{
            case attribute: Attribute => {
              if (allRights.contains(attribute)) {
                myLeft = individualCondition(1)
                myRight = individualCondition(0)
              }
              else {
                myLeft = individualCondition(0)
                myRight = individualCondition(1)
              }
              attribute
            }
          }
        }
      })
    }
//    println("Name of left "+myLeft.asInstanceOf[AttributeReference].name)
//    println("Name of right "+myRight.asInstanceOf[AttributeReference].name)
    resultExp = resultExp :+ myLeft
    resultExp = resultExp :+ myRight
    resultExp
  }

  override def hasNext: Boolean = {
    //    println("do Hash Join has next?")
    if (generalCounter < myBufferSet.length) {
      true
    } else {
      rightChild.hasNext
    }
  }

  def performHash(): Map[Int, Seq[InternalRow]] = {
    var hashMyJoin: Map[Int, Seq[InternalRow]] = Map[Int, Seq[InternalRow]]()
    println("AllKeys "+AllHashMaps.keys)
    println("parent "+resultExp(0).asInstanceOf[AttributeReference].qualifier)
    println("current key "+(resultExp(0).asInstanceOf[AttributeReference].name+resultExp(0).asInstanceOf[AttributeReference].qualifier(0)).toLowerCase)
    if(AllHashMaps.contains((resultExp(0).asInstanceOf[AttributeReference].name+resultExp(0).asInstanceOf[AttributeReference].qualifier(0)).toLowerCase)){
      println("in ")
      hashMyJoin = AllHashMaps((resultExp(0).asInstanceOf[AttributeReference].name+resultExp(0).asInstanceOf[AttributeReference].qualifier(0)).toLowerCase)
    }else{
      val allRows: Seq[InternalRow] = leftChild.toSeq.filter(_ != null)
      hashMyJoin = allRows.groupBy(LeftBoundCondition.asInstanceOf[Expression].eval(_).hashCode())
    }
//    val allRows: Seq[InternalRow] = leftChild.toSeq.filter(_ != null)
//    hashMyJoin = allRows.groupBy(LeftBoundCondition.asInstanceOf[Expression].eval(_).hashCode())
    hashMyJoin
  }




  def getNewBuffer(): Seq[InternalRow] = {
    if(AllHashMaps.contains((resultExp(1).asInstanceOf[AttributeReference].name+resultExp(1).asInstanceOf[AttributeReference].qualifier(0)).toLowerCase)){
      println("in ")
      var buffer: Seq[InternalRow] = Seq[InternalRow]()
      val myhashJoin = AllHashMaps((resultExp(1).asInstanceOf[AttributeReference].name+resultExp(1).asInstanceOf[AttributeReference].qualifier(0)).toLowerCase)
      myhashJoin.values.map(row => {
        val combinedROW = new JoinedRow(row, rightRow)
        //println("Print combine "+combinedROW)
        //println("BC "+boundJoinCondition)
        if (boundJoinCondition.eval(combinedROW).asInstanceOf[Boolean]) {
          //System.err.println("I am inside  "+buffer)
          buffer = buffer :+ combinedROW
          buffer
        }
      })
    }else {
      var buffer: Seq[InternalRow] = Seq[InternalRow]()
      val rightRow = rightChild.next()
      //println("MyRifht "+rightRow)
      //System.err.println("ROW from NJ" + rightRow)
      if (rightRow != null) {
        val rightKey = rightBoundCondition.asInstanceOf[Expression].eval(rightRow)
        val HashKey = rightKey.hashCode()
        // System.err.println("Key "+HashKey)
        val hashValue = hashJoin.get(HashKey)
        //System.err.println("value we get in map "+hashValue)
        if (hashValue != None && hashValue != null) {
          hashValue.get.map(row => {
            val combinedROW = new JoinedRow(row, rightRow)
            //println("Print combine "+combinedROW)
            //println("BC "+boundJoinCondition)
            if (boundJoinCondition.eval(combinedROW).asInstanceOf[Boolean]) {
              //System.err.println("I am inside  "+buffer)
              buffer = buffer :+ combinedROW
              buffer
            }
          })
        }
      } else {
        buffer = buffer :+ null
      }
      buffer
    }
  }

  override def next(): InternalRow = {
    var newRow: InternalRow = null
    if (generalCounter < myBufferSet.length) {
      newRow = myBufferSet(generalCounter)
      generalCounter = generalCounter + 1
    } else {
      myBufferSet = null
      myBufferSet = getNewBuffer()
      generalCounter = 0
      if (generalCounter < myBufferSet.length) {
        newRow = myBufferSet(generalCounter)
        generalCounter = generalCounter + 1
      }
    }
    newRow
  }
}
