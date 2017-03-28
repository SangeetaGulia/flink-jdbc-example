import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class JDBCJavaExample {

  public static void main(String[] args) throws Exception {
    String driverName = "org.apache.hive.jdbc.HiveDriver";
    String dbURL = "jdbc:hive2://localhost:10000/default";
    String query = "select * from interns";

    final TypeInformation<?>[] fieldTypes =
        new TypeInformation<?>[] { BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO };

    final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
        JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName).setDBUrl(dbURL)
            .setQuery(query).setRowTypeInfo(rowTypeInfo);

    DataSet<Row> source = environment.createInput(inputBuilder.finish());

    source.print();

  }

}
