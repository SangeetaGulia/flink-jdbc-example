import org.apache.flink.CarbonDataOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CarbonJDBCExample {
    public static void main(String[] args) throws Exception {
        String username = "root";
        String password = "root";
        String driverName = "com.mysql.cj.jdbc.Driver";
        String dbURL = "jdbc:mysql://localhost:3306/default";
        String query = "select * from interns";
        String sourcePath = "/home/sangeeta/projects/huawei/flink-jdbc-example/src/main/resources/result.txt";
        String destinationPath = "hdfs://localhost:54310/user/hduser/input-files/flinkdemo.csv";
        final TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
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
        List<Row> sourceList = source.collect();
        Iterator iter = sourceList.iterator();
        List<InternRecord> internRecords = new ArrayList<>();
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(sourcePath));
            while (iter.hasNext()) {
                out.println(iter.next().toString());
            }
        } catch(Exception ex) {
            System.out.println("Exception occured" + ex);
        } finally {
            out.close();
        }

        HadoopHandler hadoopHandler = new HadoopHandler();
        try {
            hadoopHandler.writeToHDFS(sourcePath, destinationPath);
        } catch (Exception ex) {
            System.out.println("Hadoop connection failed" + ex);
        }

        CarbonDataOutputFormat.CarbonDataOutputFormatBuilder carbonBuilder = CarbonDataOutputFormat
                .buildCarbonDataOutputFormat()
                .setMasterUrl("spark://sangeeta:7077")
                .setStoreLocation("hdfs://localhost:54310/user/hive/warehouse/carbon.store")
                .setQuery("Load data inpath " + destinationPath + "into table new_interns options('DELIMITER'=',','FILEHEADER'='id,name')" );

        source.output(carbonBuilder.finish());
        environment.execute();
        source.print();
    }
}
