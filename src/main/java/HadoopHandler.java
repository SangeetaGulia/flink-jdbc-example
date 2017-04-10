import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HadoopHandler {

    public void writeToHDFS(String sourcePath, String destinationPath) throws IOException {
         Configuration conf = new Configuration();
         Path hdfsCoreSitePath = new Path("/home/hduser/hadoop/hadoop-2.7.3/etc/hadoop/core-site.xml");
         Path hdfsHDFSSitePath = new Path("/home/hduser/hadoop/hadoop-2.7.3/etc/hadoop/hdfs-site.xml");
         conf.addResource(hdfsCoreSitePath);
         conf.addResource(hdfsHDFSSitePath);
        FileSystem fileSystem = FileSystem.get(conf);

        saveFile(sourcePath, destinationPath, conf, fileSystem);
        readFileFromHdfs(fileSystem, destinationPath);
        fileSystem.close();
    }

    public void saveFile(String sourcePath, String destinationPath, Configuration conf, FileSystem fileSystem) throws IOException {
        Path inputPath = new Path(sourcePath);
        Path outputPath = new Path(destinationPath);
        fileSystem.copyFromLocalFile(inputPath, outputPath);
    }

    public void readFileFromHdfs(FileSystem fileSystem, String destinationPath) throws IOException {
        Path hdfsPath = new Path(destinationPath);
        Path localPath = new Path("/home/sangeeta/projects/huawei/flink-jdbc-example/src/main/resources/copiedToLocal.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

}
