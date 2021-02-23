import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Map;

/**
 * Created on 2021/2/22.
 *
 * @author ${AUTHOR}
 */
public class HdfsClientTest {

    @Test
    public void TestListDir() throws URISyntaxException, IOException, InterruptedException {
        //hdfs配置
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://base1:9000");
        //获取hdfs文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://base1:9000"), configuration, "root");
        //遍历文件系统
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/herminen"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath().getName());
        }
        fileSystem.close();
    }

    @Test
    public void testUploadFile() throws URISyntaxException, IOException, InterruptedException {
        //配置信息
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://base1:9000");
        //获取文件系统对象
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://base1:9000"), configuration, "root")){
            //上传文件
            fileSystem.copyFromLocalFile(new Path("E:\\soft\\error.log"), new   Path("/herminen/testdata/error.log"));
            System.out.println("upload completly!");

        }
    }

    @Test
    public void testDeleteFile() throws URISyntaxException, IOException, InterruptedException {
        //配置信息
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://base1:9000");
        //文件系统
        try(FileSystem fileSystem = FileSystem.get(new URI("hdfs://base1:9000"), configuration, "root")) {
            System.out.println("删除文件执行结束，结果：" + fileSystem.delete(new Path("/herminen/error.log"), true));
        }
    }

}
