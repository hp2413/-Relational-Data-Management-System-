package microbase


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate
import scala.io.Source

class AllDataTableIterator(table :MainClass) extends Iterator[InternalRow]{

  override def hasNext: Boolean ={
    if (count < listLenght){
      true
    }else{
      count = 0
      false
    }
  }

  override def next(): InternalRow = {
    count += 1
    result(count-1)
  }

  var count : Int = 0
  val result : IndexedSeq[InternalRow] = fetchAllTableData().toIndexedSeq
  val listLenght : Int = result.length

  def fetchAllTableData() : Seq[InternalRow] ={
    val dataTypes = table.output.map { field: Attribute => field.dataType }
    // Read the file in
    val iterator  =  Source.fromFile(table.location.get).getLines.map { line: String =>
      val row = line.split("\\|")
      val parsed = dataTypes.zip(row).map { case (dataType: DataType, field: String) =>
        parseField(field, dataType)
      }
      InternalRow.fromSeq(parsed)
    }
    iterator.toSeq
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

}
