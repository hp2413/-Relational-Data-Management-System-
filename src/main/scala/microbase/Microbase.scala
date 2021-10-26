package microbase
import microbase.Microbase.{doMyAggregate, doMyFilter, doMyLimit}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.execution.SparkSqlParser

import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.{Inner => InnerJoin}

import scala.io.Source
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}

object Microbase {

  // create map variables to map all the tables in the file
  var tablesMap = new mutable.HashMap[Seq[String], MainClass]()
  var processedTablesMap = new mutable.HashMap[MainClass, AllDataTableIterator]()
  val rules = Seq[OptimizationRule](PushDownFilter, PushDownJoin, PushDownProject)
  var AllHashMaps = new mutable.HashMap[String, Map[Int, Seq[InternalRow]]]()

  def main(args: Array[String]): Unit = {

    while (true) {
      System.out.print("$>\n")
      val myline: String = scala.io.StdIn.readLine()
      // convert sql to logical plan

      try {

        val parseString = new SparkSqlParser().parsePlan(myline)

        // print(parseString);
        parseString match {
          case c: CreateTableStatement =>
            // getting instance of create table
            val newObj = MainClass(tableName = c.tableName, tableSchema = c.tableSchema, location = c.location,hash_index = c.options.get("hash_index") , tree_index = c.options.get("primary_key"), primary_key = c.options.get("tree_index"))
            // setting the instance to hashmap
            processedTablesMap.put(newObj, new AllDataTableIterator(table = newObj))
            val primaryKeys = c.options.get("primary_key").get.split(',')
            primaryKeys.map( eachKey => {
              val attributeRef = newObj.NewOutput.filter( _.name == (eachKey).toUpperCase)
              if(attributeRef.nonEmpty){
                val BR = bindReferences(attributeRef(0).asInstanceOf[Expression], newObj.NewOutput)
                val hashJoin : Map[Int, Seq[InternalRow]] = processedTablesMap(newObj).result.groupBy(BR.asInstanceOf[Expression].eval(_).hashCode())
                AllHashMaps.put((eachKey+c.tableName(0)).toLowerCase, hashJoin)
              }
            })
            tablesMap.put(c.tableName, newObj)
            //    processedTablesMap foreach(println)
          case _ =>
            //            println(parseString.treeString)
            //            System.err.println("Unresolved ")
            //            System.err.println(parseString.treeString)

            // resolve unresolved relation
            val resolveRelation = parseString.transformUp {
              case UnresolvedRelation(nameElements, _, _) =>
                val tbObj = tablesMap(nameElements)
                new MainClass(tableName = tbObj.tableName, tableSchema = tbObj.tableSchema, location = tbObj.location,hash_index = tbObj.hash_index, tree_index = tbObj.tree_index, primary_key = tbObj.primary_key)
            }

            // resolve unresolved attribute
            val resolvedAll = resolveRelation.transformUp {
              // project operation
              case project: Project => doMyProject(project)

              // Join operation
              case join: Join => doMyJoin(join)

              // Filter operation
              case filter: Filter => doMyFilter(filter)

              // Aggregate operation
              case aggregate: Aggregate => doMyAggregate(aggregate)

              // sort operation
              case sort: Sort => doMySort(sort)

              // limit operation
              case globalLimit: GlobalLimit => doMyLimit(globalLimit)
            }
            // println(resolvedAll)
//                        System.err.println("Resolved ")
//                        System.err.println(resolvedAll)

            // to optimize the query
            val optimizedQuery = optimizerPoint(resolvedAll)

            //println(optimizedQuery)

//                      System.err.println("Optimized ")
//                      System.err.println(optimizedQuery)

            if (optimizedQuery.resolved) {
              evalMyQueryPlan(optimizedQuery)
            }
        }

      }
      catch {
        //        case e: Throwable => System.err.print("error "+e);

        case _: Throwable => System.err.print("");

      }
    }
  }


  // apply the optimization rule on logical plan on repeat
  def onePass(plan: LogicalPlan): LogicalPlan = {
    var current = plan
    for (rule <- rules) {
      current = rule.apply(current)
    }
    current
  }

  // recursively call to optimize the logical plan
  def optimizerPoint(plan: LogicalPlan): LogicalPlan = {
    var current = plan
    var last: LogicalPlan = null
    while (last == null || !current.fastEquals(last)) {
      last = current
      current = onePass(current)
    }
    current
  }

  // split and expression function
  def splitAnds(expr: Expression): Seq[Expression] = {
    expr match {
      case And(a, b) => splitAnds(a) ++ splitAnds(b)
      case _ => Seq(expr)
    }
  }

  // split equal expression function
  def splitEqual(expr: Expression): Seq[Expression] = {
    expr match {
      case EqualTo(a, b) => splitEqual(a) ++ splitEqual(b)
      case _ => Seq(expr)
    }
  }

  // split expression function
  def splitOR(expr: Expression): Seq[Expression] = {
    expr match {
      case Or(a, b) => splitOR(a) ++ splitOR(b)
      case _ => Seq(expr)
    }
  }

  // run and get output
  def evalMyQueryPlan(logicalNode: LogicalPlan): Unit = {
//    val start = System.currentTimeMillis()
    val iteratorValue = getGeneralIterator(logicalNode)
    while (iteratorValue.hasNext) {
      val dataType = logicalNode.output.map(d => d.dataType)
      val newRow = iteratorValue.next()
      if (newRow != null) {
        val newResult = newRow.toSeq(dataType)
        var i = 0
        var count = 0
        var result: String = null
        newResult.foreach(res1 => {
          dataType(count) match {
            case date: DateType => {
              val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
              val newRes = res1.asInstanceOf[Int]
              val myDate = LocalDate.ofEpochDay(newRes).format(dateFormat)
              if (i.equals(0)) {
                result = myDate
                count = count + 1
                i = i + 1
              } else {
                result = result + "|" + myDate
                count = count + 1
              }
            }
            case _ => {
              if (i.equals(0)) {
                result = res1.toString
                count = count + 1
                i = i + 1
              } else {
                result = result + "|" + res1.toString
                count = count + 1
              }
            }
          }
        })
        System.out.println(result)
      }
    }
  }

  def getGeneralIterator(myPlan: LogicalPlan): Iterator[InternalRow] = {
    myPlan match {
      // Project Iterator
      case project: Project =>
        val boundRefs = project.projectList.map { bindReferences(_, project.child.output) }
        new ProjectIterator(boundCondition = boundRefs, child = getGeneralIterator(project.child))

      // Filter Iterator
      case filter: Filter =>
        val boundRefs = bindReferences(filter.condition, filter.child.output)
        new FilterIterator(boundCondition = boundRefs, child = getGeneralIterator(filter.child))

      // Join Iterator
      case join: Join =>
        if (join.condition == None) {
          // System.err.println("calling from cross join when condition is "+join.condition)
          new JoinIterator(leftChild = getGeneralIterator(join.left), rightChild = getGeneralIterator(join.right))
        }else{
          // System.err.println("calling from Hash join - Condition "+join.condition)
          val boundRefs = bindReferences(join.condition.get, join.left.output ++ join.right.output)
          new HashJoinIterator(boundJoinCondition = boundRefs, condition = join.condition.get, leftChild = getGeneralIterator(join.left), rightChild = getGeneralIterator(join.right), allLeft = join.left.output , allRights = join.right.output)
        }

      // GlobalLimit Iterator
      case globalLimit: GlobalLimit =>
        val boundRefs: Expression = bindReferences( globalLimit.limitExpr, globalLimit.child)
        new LimitIterator(child = getGeneralIterator(globalLimit.child), boundCondition = boundRefs)

      // Distinct Iterator
      case distinct: Distinct => getGeneralIterator(distinct.child)

      // SubQueryAlias Iterator
      case subQueryAlias: SubqueryAlias => getGeneralIterator(subQueryAlias.child)

      // Sort Iterator
      case sort: Sort =>
        val boundRefs: Seq[Expression] = sort.order.map( eachCondition => { bindReferences(eachCondition.child, sort.child.output)})
        new SortIterator(condition = sort.order, child = getGeneralIterator(sort.child), boundCondition = boundRefs)

      // Aggregate Iterator
      case aggregate: Aggregate =>
        new AggregateIterator(aggregateExpressions = aggregate.aggregateExpressions, groupingExpressions = aggregate.groupingExpressions, child = getGeneralIterator(aggregate.child), lPlan = aggregate.child)

      // Union Iterator
      case union: Union =>{
        val children = union.children.map { child =>  getGeneralIterator(child) }
        var output = children.head
        for(next <- children.tail) {
          output = output ++ next
        }
        output
      }

      // Table Iterator
      case table: MainClass => processedTablesMap(table)

//      case table: MainClass => val dataTypes = table.output.map { field: Attribute => field.dataType }
//        // Read the file in
//        Source.fromFile(table.location.get).getLines.map { line:String =>
//          val row = line.split("\\|")
//          val parsed = dataTypes.zip(row).map { case (dataType:DataType, field:String) =>
//            parseField(field, dataType)
//          }
//          InternalRow.fromSeq(parsed)
//        }
    }
  }

  def parseField(value: String, dataType: DataType): Any =
  {
    dataType match {
      case StringType => UTF8String.fromString(value)
      case IntegerType => value.toInt
      case LongType => value.toLong
      case FloatType => value.toFloat
      case DoubleType => value.toDouble
      case DateType => { val elements = value.split("-");
        Math.toIntExact(
          LocalDate.of(elements(0).toInt,
            elements(1).toInt,
            elements(2).toInt).toEpochDay
        )
      }
    }
  }

  def bindReferences(expr: Expression, inputs: Seq[Attribute]): Expression =
  {
    val inputPosition = inputs.map{_.exprId}.zipWithIndex.toMap
    expr.transformDown{
      case a:AttributeReference => BoundReference(inputPosition(a.exprId), a.dataType, a.nullable)
    }
  }

  def bindReferences(expr: Expression, plan: LogicalPlan): Expression = bindReferences(expr, plan.output)

  // limit function
  def doMyLimit(globalLimit: GlobalLimit): GlobalLimit = {
    GlobalLimit(globalLimit.limitExpr, globalLimit.child.asInstanceOf[LocalLimit].child)
  }

  // sort function
  def doMySort(sort: Sort): Sort = {
    //println("SortDetail "+sort.order)
    var sortedOrderResult: Seq[SortOrder] = Seq[SortOrder]()

    sort.order.map({ myOrder =>
      //      println("Sort order exp before  "+myOrder.child)
      //      println("Child "+sort.child.outputSet.toList)
      //      println("Sorted list "+sort.child.output.toList)
      val result = myOrder.child.transform {
        //  unresolved attributes
        case unresolvedAttribute: UnresolvedAttribute => {
          var myResolvedAtr: Attribute = null
          val nameList = unresolvedAttribute.name.split('.')
          sort.child.output.toList.map(artN => {
            if (nameList.length >= 2) {
              val tName = Seq(unresolvedAttribute.nameParts(0))
              val cName = unresolvedAttribute.nameParts(1)
              // println(artN.qualifier)
              if (artN.name.equalsIgnoreCase(cName) && artN.qualifier.equals(tName)) {
                myResolvedAtr = artN
                myResolvedAtr
              }
            }
            else {
              if (artN.name.equalsIgnoreCase(unresolvedAttribute.name)) {
                myResolvedAtr = artN
                myResolvedAtr
              }
            }
          })
          myResolvedAtr
        }
      }
      sortedOrderResult = sortedOrderResult :+ SortOrder(child = result, direction = myOrder.direction, sameOrderExpressions = myOrder.sameOrderExpressions, nullOrdering = myOrder.nullOrdering)
      //  println("Final list   "+sortedOrderResult)
      sortedOrderResult
    })
    //    println("Last list   "+sortedOrderResult)
    Sort(sortedOrderResult, sort.global, sort.child)
  }

  // aggregate function
  def doMyAggregate(aggregate: Aggregate): Aggregate = {
    //   println("groupingExpressions "+aggregate.groupingExpressions)
    //   println("aggregateExpressions "+aggregate.aggregateExpressions)
    //    println("child "+aggregate.child)
    var resolvedGroupingExpressions: Seq[Expression] = Seq[Expression]()
    resolvedGroupingExpressions = getMyAttributeExpressionResolved(aggregate.groupingExpressions, aggregate.child)

    var resolvedAttributeSeq: Seq[NamedExpression] = Seq[NamedExpression]()
    resolvedAttributeSeq = getMyAttributesResolved(aggregate.aggregateExpressions, aggregate.child)

    Aggregate(resolvedGroupingExpressions, resolvedAttributeSeq, aggregate.child)
  }

  // get unresolved expression resolved
  def getMyAttributeExpressionResolved(attributeList: Seq[Expression], child: LogicalPlan): Seq[Expression] = {

    var resolvedAttributeSeq: Seq[Expression] = Seq[Expression]()

    attributeList.map({ exp =>
      val resultExp = exp.transformUp {
        //  unresolved attributes
        case unresolvedAttribute: UnresolvedAttribute => {
          var myResolvedAtr: Attribute = null
          val nameList = unresolvedAttribute.name.split('.')
          child.output.toList.map(artN => {
            if (nameList.length >= 2) {
              val tName = Seq(unresolvedAttribute.nameParts(0))
              val cName = unresolvedAttribute.nameParts(1)
              // println(artN.qualifier)
              if (artN.name.equalsIgnoreCase(cName) && artN.qualifier.equals(tName)) {
                myResolvedAtr = artN
                myResolvedAtr
              }
            }
            else {
              if (artN.name.equalsIgnoreCase(unresolvedAttribute.name)) {
                myResolvedAtr = artN
                myResolvedAtr
              }
            }
          })
          myResolvedAtr
        }
      }
      //println("myResolved "+resultExp)
      resolvedAttributeSeq = resolvedAttributeSeq :+ resultExp.asInstanceOf[Expression]
      resolvedAttributeSeq
    })

    resolvedAttributeSeq
  }

  // get unresolved namedExpression resolved
  def getMyAttributesResolved(attributeList: Seq[NamedExpression], child: LogicalPlan): Seq[NamedExpression] = {

    var resolvedAttributeSeq: Seq[NamedExpression] = Seq[NamedExpression]()

    attributeList.map({ exp =>
      val resultExp = exp.transformUp {
        //  unresolved function
        case unresolvedFunction: UnresolvedFunction => {
          //          println("name "+ unresolvedFunction.name)
          //          println("arguments "+ unresolvedFunction.arguments)
          //          println("isDistinct "+ unresolvedFunction.isDistinct)
          //          println("filter "+ unresolvedFunction.filter)
          val builder =
          FunctionRegistry.builtin
            .lookupFunctionBuilder(unresolvedFunction.name)
            .getOrElse {
              throw new RuntimeException(
                //s"Unable to resolve function `${unresolvedFunction.name}`"
              )
            }
          //println(unresolvedFunction.arguments.toList)
          builder(unresolvedFunction.arguments) // returns the replacement expression node.
        }

        //  unresolved alias
        case unresolvedAlias: UnresolvedAlias => {
          //          println("child "+ unresolvedAlias.child)
          //          println("aliasFunc "+ unresolvedAlias.aliasFunc)
          Alias(unresolvedAlias.child, Random.alphanumeric.filter(_.isLetter).take(3).mkString)(NamedExpression.newExprId, Seq.empty, None, Seq.empty)
        }

        //  unresolved attributes
        case unresolvedAttribute: UnresolvedAttribute => {
          var myResolvedAtr: Attribute = null
          val nameList = unresolvedAttribute.name.split('.')
          child.output.toList.map(artN => {
            if (nameList.length >= 2) {
              val tName = Seq(unresolvedAttribute.nameParts(0))
              val cName = unresolvedAttribute.nameParts(1)
              // println(artN.qualifier)
              if (artN.name.equalsIgnoreCase(cName) && artN.qualifier.equals(tName)) {
                myResolvedAtr = artN
                myResolvedAtr
              }
            }
            else {
              if (artN.name.equalsIgnoreCase(unresolvedAttribute.name)) {
                myResolvedAtr = artN
                myResolvedAtr
              }
            }
          })
          myResolvedAtr
        }
      }
      //println("myResolved "+resultExp)
      resolvedAttributeSeq = resolvedAttributeSeq :+ resultExp.asInstanceOf[NamedExpression]
      resolvedAttributeSeq
    })

    var FinalAttributeSeq: Seq[NamedExpression] = Seq[NamedExpression]()
    // resolving the unresolved star

    resolvedAttributeSeq.toList.map({ exp =>
      if (exp.isInstanceOf[UnresolvedStar]) {
        val tempVal = exp.asInstanceOf[UnresolvedStar]
        if (tempVal.target.isEmpty) {
          FinalAttributeSeq = child.output.toList
          FinalAttributeSeq
        }
        else {
          val tName = tempVal.target.get(0)
          child.output.toList.map(artN => {
            //            println("Name of the table "+tName)
            if (artN.qualifier.equals(Seq(tName))) {
              //              println("ListValue"+artN)
              FinalAttributeSeq = FinalAttributeSeq :+ artN
            }
          })
        }
      }
      else {
        FinalAttributeSeq = FinalAttributeSeq :+ exp
        FinalAttributeSeq
      }
    })

    FinalAttributeSeq
  }

  // project function
  def doMyProject(project: Project): Project = {
    //println("ProjectList "+getMyAttributesResolved(project.projectList, project.child))
    //println("Project child is resolved "+ project.child.resolved)
    Project(getMyAttributesResolved(project.projectList, project.child), project.child)
  }

  // Join function
  def doMyJoin(j: Join): Join = {
    if (j.condition == None) {
      Join(left = j.left, right = j.right, joinType = j.joinType, condition = None, hint = j.hint)
    } else {
      //      println("Condition "+j.condition)
      val resJ = j.condition.get.transform {
        //  Unresolved attributes
        case unresolvedAttribute: UnresolvedAttribute => {
          val tName = Seq(unresolvedAttribute.nameParts(0))
          val cName = unresolvedAttribute.nameParts(1)
          var myResolvedAtr: Attribute = null
          j.output.foreach(artN => {
            //println(artN.qualifier)
            if (artN.name.equalsIgnoreCase(cName) && artN.qualifier.equals(tName)) {
              myResolvedAtr = artN
            }
          })
          myResolvedAtr
        }
      }
      Join(left = j.left, right = j.right, joinType = j.joinType, condition = Some(resJ), hint = j.hint)
    }
  }

  // Filter function
  def doMyFilter(f: Filter): Filter = {
    val resF = f.condition.transform {
      case urft: UnresolvedAttribute => {
        if (urft.nameParts.length >= 2) {
          val tName = Seq(urft.nameParts(0))
          val cName = urft.nameParts(1)
          var myftOP: Attribute = null
          f.child.output.foreach(artN => {
            if (artN.name.equalsIgnoreCase(cName) && artN.qualifier.equals(tName)) {
              myftOP = artN
            }
          })
          myftOP
        }
        else {
          val cName = urft.nameParts(0)
          var myftOP: Attribute = null
          f.child.output.foreach(artN => {
            if (artN.name.equalsIgnoreCase(cName)) {
              myftOP = artN
            }
          })
          myftOP
        }
      }
    }
    Filter(resF, f.child)
  }

}