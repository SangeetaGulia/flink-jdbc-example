import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.Row;

public class JDBCJavaExample {

  public static void main(String[] args) throws Exception {
/*

  // CONFIGURATION SETTINGS FOR HIVE

    String driverName = "org.apache.hive.jdbc.HiveDriver";
    String dbURL = "jdbc:hive2://localhost:10000/default";
    String username="scott";
    String password="tiger";
    */

  // CONFIGURATION SETTINGS WITH MYSQL

    String username="root";
    String password="root";
    String driverName = "com.mysql.cj.jdbc.Driver";
    String dbURL = "jdbc:mysql://localhost:3306/default";

    String query = "select * from interns";

    final TypeInformation<?>[] fieldTypes =
        new TypeInformation<?>[] { BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO };

    final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    JDBCInputFormat.JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername(driverName)
            .setUsername(username)
            .setPassword(password)
            .setDBUrl(dbURL)
            .setQuery(query)
            .setRowTypeInfo(rowTypeInfo);

    DataSet<Row> source = environment.createInput(inputBuilder.finish());
    source.print();

    JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder = JDBCOutputFormat
            .buildJDBCOutputFormat()
            .setDrivername(driverName)
            .setUsername(username)
            .setPassword(password)
            .setDBUrl(dbURL)
            .setQuery("insert into new_interns(id, name) values(?,?)");

    source.output(outputBuilder.finish());
    environment.execute();
    source.print();
  }
}
