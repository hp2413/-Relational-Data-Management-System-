package microbase

import microbase.Microbase.splitAnds
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}

object PushDownJoin extends  OptimizationRule {

  override def apply(plan: LogicalPlan) : LogicalPlan ={
    val newPlan = plan.transform{
      case Join(left, right, joinType, condition , hint) => {
        if (condition != None){
          // println(left.maxRows.get)
          var  myLeft : LogicalPlan = left
          var  myRight: LogicalPlan = right
          var  myCondition : Option[Expression] = None
          var  leftExp : Expression = null
          var  rightExp : Expression = null
          var  mainExp : Expression = null
          var  filterCondition : Expression = null
          val myConditionList = splitAnds(condition.get)
          myConditionList.foreach(exp => {
            if (exp.references.subsetOf(left.outputSet)){
              if (leftExp != null){
                leftExp = And(leftExp,exp)
              }else{
                leftExp = exp
              }
            }else if(exp.references.subsetOf(right.outputSet)){
              if (rightExp != null){
                rightExp = And(rightExp,exp)
              }else{
                rightExp = exp
              }
            }
            else{
              if( !exp.isInstanceOf[LessThan] && !exp.isInstanceOf[GreaterThan] ) {
                if (mainExp != null) {
                  mainExp = And(mainExp, exp)
                } else {
                  mainExp = exp
                }
              }else{
                if (filterCondition != null) {
                    filterCondition = And(filterCondition, exp)
                }else{
                    filterCondition = exp
                }
              }
            }
          })
          if (leftExp != null){
            myLeft = Filter(condition = leftExp, child = left)
          }
          if(rightExp != null){
            myRight = Filter(condition = rightExp, child = right)
          }
          if(mainExp != null){
            myCondition = Some(mainExp)
          }
          if(filterCondition != null){
            Filter(filterCondition, Join(left = myLeft, right = myRight, joinType = joinType, condition =  myCondition, hint = hint))
          }else {
            Join(left = myLeft, right = myRight, joinType = joinType, condition =  myCondition, hint = hint)
          }
        }else{
          Join(left = left, right = right, joinType = joinType, condition =  condition, hint = hint)
        }
      }
    }
    newPlan
  }
}

