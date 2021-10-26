package microbase

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import microbase.Microbase.bindReferences

import scala.collection.mutable

class AggregateIterator(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: Iterator[InternalRow], lPlan : LogicalPlan) extends  Iterator[InternalRow]{

  var index: Integer = 0
  var finalEvalExpMap : mutable.HashMap[Expression, Expression] = mutable.HashMap.empty
  var boundGroups : Seq[Expression] = groupingExpressions.map( eachCondition => { bindReferences( eachCondition, lPlan.output) })
  var allBoundRef: mutable.HashMap[Expression,Expression] = getAllBoundRefrences(aggExp = aggregateExpressions)
  var bufferLocation : mutable.HashMap[String,Int] = getBufferLocation(aggExp = aggregateExpressions)
  var finalData:  Seq[InternalRow] = getAllRows()

  override def hasNext: Boolean = {
    if(index < finalData.length){
      true
    }else{
      false
    }
  }
  override def next(): InternalRow = {
//    println("Final o/p After agg:"+index)
    val row = finalData(index)
    index += 1
    row
  }

  def getAllBoundRefrences( aggExp : Seq[NamedExpression] ): mutable.HashMap[Expression,Expression] ={
    val allBR: mutable.HashMap[Expression, Expression] = mutable.HashMap.empty
    aggExp.map(exp => {
      exp match {
        case  Alias(myAggr : DeclarativeAggregate, name ) =>{
          val aggBufferAttributes = myAggr.aggBufferAttributes
          myAggr.updateExpressions.foreach{ eachExp =>{
            allBR.put(key = eachExp, value =  bindReferences(expr = eachExp, aggBufferAttributes ++ lPlan.output) )
          }
          }
          finalEvalExpMap.put(key = myAggr.evaluateExpression, value =  bindReferences(expr = myAggr.evaluateExpression, aggBufferAttributes ++ lPlan.output))
        }
        case a : Attribute => allBR.put(key = exp, value =  bindReferences(expr = exp, inputs = lPlan.output) )
      }
    })
    //System.err.println("Time: "+(time-time1))
    //println("all BR: "+allBR)
    //println("all Final: "+finalEvalExpMap)
    //System.err.println("all BR: "+allBR)
    //System.err.println("End  of BR ")
    allBR
  }

  def getBufferLocation(aggExp : Seq[NamedExpression]): mutable.HashMap[String,Int] ={
    var count = 0
    val buffLocation: mutable.HashMap[String, Int] = mutable.HashMap.empty
    aggExp.foreach(exp => {
      exp match {
        case  Alias(myAggregate : DeclarativeAggregate, name ) => {
          buffLocation.put(key = name, value = count)
          count = count + 1
        }
        case attribute: Attribute => {
          buffLocation.put(key = attribute.toString(), value =  count)
          count = count + 1
        }
      }
    })
    //println("Buffer Location "+buffLocation)
    buffLocation
  }

  def getAllRows() : Seq[InternalRow] = {
    //System.err.println("Start of getAllRows ")
    var FinalResultIterator: Seq[InternalRow] = Seq[InternalRow]()
    val allGroupByValues: mutable.HashMap[InternalRow,Seq[Seq[Any]]] =  mutable.HashMap.empty
    while (child.hasNext){
      val newValue = child.next()
      if (newValue != null){
        //System.err.println("Child Value "+newValue)
        //System.err.println("boundGroups "+boundGroups)
        var resultOfGrpExp: Seq[Any] = Seq[Any]()
        boundGroups.map( myExp =>{
          //System.err.println("myExp " + myExp)
          resultOfGrpExp = resultOfGrpExp :+ myExp.eval(newValue)
          resultOfGrpExp
          // System.err.println("resultOfGrpExp "+resultOfGrpExp)
        })
        val key =  InternalRow.fromSeq(resultOfGrpExp)
        //println("Key from All data "+key)
        //System.err.println("Key from All data "+key)
        if (allGroupByValues.get(key) != None){
          // key is already present
          allGroupByValues(key = key) = operateAllAggrFunctions(mapValues = newValue, lastSeq = allGroupByValues(key))
          //println("repeated Value from alldata "+allGroupByValues)
        }else{
          // create a new key
          allGroupByValues.put(key = key, value = operateAllAggrFunctions(mapValues = newValue, lastSeq = null))
          //println("New Value from alldata "+allGroupByValues)
        }
        //System.err.println("Value from all: "+allGroupByValues)
      }
    }
    //    println("PreFinal Buffer "+allGroupByValues)
    allGroupByValues.keys.toList.map( eachKey => {
      var resultOfGrpExp: Seq[Any] = Seq[Any]()
      aggregateExpressions.map( aggExp => {
        aggExp match {
          case attribute: Attribute =>{
            //println("Before eval value of the attr "+allGroupByValues(eachKey))
            resultOfGrpExp = resultOfGrpExp :+ allGroupByValues(eachKey)(bufferLocation(attribute.toString()))(0)
            //println("Final value of the attr "+allGroupByValues(eachKey)(bufferLocation(attribute.toString()))(0))
            resultOfGrpExp
          }
          case Alias (myAggr : DeclarativeAggregate, name ) =>{
            val finalBuffer = InternalRow.fromSeq(allGroupByValues(eachKey)(bufferLocation(name)))
            //println("Before eval value of the func "+finalBuffer)
            resultOfGrpExp = resultOfGrpExp :+ finalEvalExpMap(myAggr.evaluateExpression).eval(finalBuffer)
            //println("Final value of the func "+finalEvalExpMap(myAggr.evaluateExpression).eval(finalBuffer))
            resultOfGrpExp
          }
        }
      })
      FinalResultIterator = FinalResultIterator :+ InternalRow.fromSeq(resultOfGrpExp)
      FinalResultIterator
    })
    //System.err.println("Time from getting row: "+(time-time1))
    //System.err.println("all Row: "+FinalResultIterator)
    FinalResultIterator
  }

  def operateAllAggrFunctions( mapValues :  InternalRow, lastSeq : Seq[Seq[Any]] ) : Seq[Seq[Any]] = {
    var FinalSeq: Seq[Seq[Any]] = Seq[Seq[Any]]()
    aggregateExpressions.toList.map( exp => {
      exp match {
        case attribute: Attribute =>{
          var resultRow: Seq[Any] = Seq[Any]()
          //println("Attribute "+attribute)
          val myBoundRef = allBoundRef(attribute)
          resultRow = resultRow :+ myBoundRef.eval(mapValues)
          FinalSeq = FinalSeq :+ resultRow
          FinalSeq
          //println("result of aggr Func "+resultRow)
        }
        case Alias (myAggr : DeclarativeAggregate, name ) =>{
          if (lastSeq != null){
            FinalSeq = FinalSeq :+ blackBoxFuction(aggr = myAggr, value = mapValues , previousValue = lastSeq(bufferLocation(name)) )
            FinalSeq
          }else{
            FinalSeq = FinalSeq :+ blackBoxFuction(aggr = myAggr, value = mapValues , previousValue = null )
            FinalSeq
          }
          //println("result of aggr Func "+resultRow)
        }
      }
    })
    FinalSeq
  }

  def blackBoxFuction( aggr : DeclarativeAggregate ,value : InternalRow, previousValue : Seq[Any]) : Seq[Any] = {
    var aggregateBuffer : InternalRow = null
    if (previousValue != null) {
      aggregateBuffer = InternalRow.fromSeq(previousValue)
    }else{
      var tempSeq : Seq[Any] = Seq[Any]()
      aggr.initialValues.map( myExp => {
        tempSeq = tempSeq :+ myExp.eval()
        tempSeq
      })
      aggregateBuffer = InternalRow.fromSeq(tempSeq)
    }
    var bufferSeq : Seq[Any] = Seq[Any]()
    val combinedROW = new JoinedRow(aggregateBuffer, value)
    //println("Alias combinedRow  "+combinedROW)
    //println("Alias updateExpressions "+aggr.updateExpressions)
    aggr.updateExpressions.map(upExp => {
      //println("exp "+upExp)
      // val myBoundRef = allBoundRef(upExp)
      //println("Only BR "+allBoundRef(upExp))
      //println("Every BR "+allBoundRef(upExp).eval(combinedROW))
      bufferSeq = bufferSeq :+ allBoundRef(upExp).eval(combinedROW)
    })
    bufferSeq
  }
}