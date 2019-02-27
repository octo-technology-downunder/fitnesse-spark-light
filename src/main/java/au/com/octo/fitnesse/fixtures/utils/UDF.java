package au.com.octo.fitnesse.fixtures.utils;

import org.apache.spark.sql.api.java.UDF1;

import java.sql.Timestamp;

public class UDF {

    public static UDF1<Timestamp, String> timestampToString = (UDF1<Timestamp, String>) timestamp -> timestamp != null ? DateFormatter.formatStandard.print(timestamp.getTime()) : null;

}
