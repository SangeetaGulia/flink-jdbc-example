//import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
//import org.apache.flink.api.java.ExecutionEnvironment
//import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
//import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
//import org.apache.flink.api.java.operators.DataSource
//import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.types.Row
//
//object JDBCExample extends App{
//
//
//
//  val env = ExecutionEnvironment.getExecutionEnvironment
//
//  val rowTypeInfo = new RowTypeInfo
//
//  val driverName = "com.mysql.jdbc.Driver"
//  val dbURL = "jdbc:mysql://localhost:3306/kafka"
//
//  val fieldTypes: Array[TypeInformation[_]] = Array (
//    BasicTypeInfo.INT_TYPE_INFO,
//    BasicTypeInfo.STRING_TYPE_INFO
//  )
//
//  val rowTypeInfo = new RowTypeInfo(fieldTypes)
//
//  val inputBuilder: JDBCInputFormatBuilder = JDBCInputFormat.buildJDBCInputFormat()
//    .setDrivername(driverName)
//    .setDBUrl(dbURL)
//    .setQuery("select * from Student")
//    .setRowTypeInfo(rowTypeInfo)
//
//
//  val source: DataSource[Row] = env.createInput(inputBuilder.finish())
//
//  source.print()
//}
