package microbase
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}

object PushDownFilter extends  OptimizationRule {
  override def apply(plan: LogicalPlan) : LogicalPlan ={
    val newPlan = plan.transform {
      case Filter(filterCondition, Join(left, right, joinType, condition, hint)) =>
        if (condition == None) {
          Join(left = left, right = right, joinType = joinType, condition = Some(filterCondition), hint = hint)
        }
        else {
          val newJoinCond: Expression = And(condition.get, filterCondition)
          Join(left = left, right = right, joinType = joinType, condition = Some(newJoinCond), hint = hint)
        }
      //case Filter(condition, MainClass(tableName, location, tableSchema)) =>
      /* and other cases here... */
    }
    newPlan
  }
}
