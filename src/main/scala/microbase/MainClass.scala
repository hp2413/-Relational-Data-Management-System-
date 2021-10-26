package microbase
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types.StructType

case class MainClass(tableName: Seq[String],
                     tableSchema: StructType,
                     location: Option[String] ,
                     hash_index : Option[String],
                     tree_index : Option[String],
                     primary_key : Option[String]) extends LeafNode{

  override def output: Seq[Attribute] = NewOutput
  val NewOutput :Seq[AttributeReference] = {
    var myOP: Seq[AttributeReference] =  Seq[AttributeReference]()
    for ( a <- 0 until tableSchema.length) {
      //      val id = NamedExpression.newExprId
      val art = AttributeReference(name = tableSchema(a).name, dataType = tableSchema(a).dataType, nullable = tableSchema(a).nullable, metadata = tableSchema(a).metadata)(qualifier = tableName)
      myOP = myOP :+ art
    }
    myOP
  }

  def fileRead() : Iterator[String] = {
      null
  }

}
