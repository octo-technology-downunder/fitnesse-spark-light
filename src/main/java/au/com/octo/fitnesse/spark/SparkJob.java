package au.com.octo.fitnesse.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.util.List;

public interface SparkJob<T> {

    List<String> describeInput();

    Dataset<T> execute(SparkSession sparkSession, Timestamp sysTimestamp);

}
