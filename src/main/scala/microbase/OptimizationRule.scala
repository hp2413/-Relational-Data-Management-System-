package microbase
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
trait OptimizationRule {
  def apply(plan: LogicalPlan): LogicalPlan
}
