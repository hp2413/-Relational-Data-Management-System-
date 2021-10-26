package microbase
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}



object PushDownProject extends OptimizationRule {
  override def apply(plan: LogicalPlan) : LogicalPlan ={
    val newPlan = plan.transform {
      case Project(projectList, child) =>{
        var isAggregateFunction = false
        projectList.foreach({ exp =>
          // println("Expression from push down "+exp)
          // println("containsChild from push down "+exp.containsChild)
          if (exp.containsChild.toList.length > 0){
            if(exp.containsChild.toList(0).isInstanceOf[AggregateFunction]){
              isAggregateFunction = true
            }
          }
        })
        // println("AggregateFunction "+isAggregateFunction)
        if(isAggregateFunction){
          Aggregate(Seq(),projectList,child)
        }else{
          Project(projectList, child)
        }
      }
      /* and other cases here... */
    }
    newPlan
  }
}