import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class JDBCJavaExample {

  public static void main(String[] args) {
    String driverName = "com.mysql.jdbc.Driver";
    String dbURL = "jdbc:mysql://localhost:3306/kafka";
    String query = "select * from Student";

    final TypeInformation<?>[] fieldTypes =
        new TypeInformation<?>[] { BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO };

    final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
        JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName).setDBUrl(dbURL)
            .setQuery(query).setRowTypeInfo(rowTypeInfo);

    DataSet<Row> source = environment.createInput(inputBuilder.finish());

    try {
      source.print();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
