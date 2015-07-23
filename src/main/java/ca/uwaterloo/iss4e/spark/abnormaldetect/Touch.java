package ca.uwaterloo.iss4e.spark.abnormaldetect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by xiuli on 7/9/15.
 */
public class Touch {


    static void touch(String filename){
        try {
            Configuration conf = new Configuration();
            FileSystem.setDefaultUri(conf, "hdfs://gho-admin:9000");
            FileSystem fileSys = FileSystem.get(conf);
            Path file = new Path(filename);
            FileStatus stat = fileSys.getFileStatus(file);
            long currentTime = System.currentTimeMillis();
            fileSys.setTimes(file, currentTime, currentTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
      Touch.touch(args[0]);
    }


}
