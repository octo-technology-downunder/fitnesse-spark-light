package au.com.octo.fitnesse.fixtures;

import au.com.octo.fitnesse.fixtures.utils.DateFormatter;
import au.com.octo.fitnesse.spark.SparkJob;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class ExecuteSparkJobBuilder {

    private SparkJob sparkJob;
    private final List datasetNames;
    private String outputFile;
    private Timestamp sysTimestamp;
    private String[] orderByColumns = new String[]{};
    private Map<String, String> config = new LinkedHashMap<>();

    public ExecuteSparkJobBuilder(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName(className);
        this.sparkJob = (SparkJob) cls.newInstance();
        this.datasetNames = sparkJob.describeInput();
    }

    public ExecuteSparkJobBuilder outputFile(String outputFile) {
        this.outputFile = outputFile.replaceAll(">", "");
        return this;
    }

    public ExecuteSparkJobBuilder sysTimestamp(String sysTimestamp) {
        if (isNotBlank(sysTimestamp))
            this.sysTimestamp = new Timestamp(DateFormatter.formatStandard.parseMillis(sysTimestamp));
        else
            this.sysTimestamp = new Timestamp(System.currentTimeMillis());
        return this;
    }

    public ExecuteSparkJobBuilder config(String config) {
        for (String keyValue : config.split("[,;]")) {
            String[] pairs = keyValue.split("=", 2);
            this.config.put(pairs[0].trim(), pairs.length == 1 ? "" : pairs[1].trim());
        }
        return this;
    }

    public ExecuteSparkJobBuilder orderByColumns(String orderByColumns) {
        this.orderByColumns = orderByColumns.split("[\\s,;]+");
        return this;
    }

    public ExecuteSparkJob build() throws Exception {
        return new ExecuteSparkJob(this);
    }

    public SparkJob getSparkJob() {
        return sparkJob;
    }

    public List getDatasetNames() {
        return datasetNames;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public Timestamp getSysTimestamp() {
        return sysTimestamp;
    }

    public String[] getOrderByColumns() {
        return orderByColumns;
    }

    public Map<String, String> getConfig() {
        return config;
    }
}
