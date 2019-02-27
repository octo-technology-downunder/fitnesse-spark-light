package au.com.octo.fitnesse.spark;

import au.com.octo.fitnesse.fixtures.utils.DateFormatter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import scala.collection.convert.Wrappers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * In readiness for CSV writing/reading, introduce functionality to:
 * - convert ArrayStruct[StructType] to string
 * - convert string to ArrayStruct[StructType]
 * - convert Row[] to string
 * - convert string to Row[]
 */
public class StructsUtil {
    private static String schemaPatternStr = "Structs\\[(\\s*\\w+\\s*=\\s*\\w+\\s*(;\\s*\\w+\\s*=\\s*\\w+\\s*)*)\\]";
    private static Pattern schemaPattern = Pattern.compile(schemaPatternStr);

    public static Dataset<Row> stringifyStrucs(Dataset<Row> ds) {
        boolean needsConversion = false;
        StructField[] orgSchemaFields = ds.schema().fields();
        StructField[] newSchemaFields = new StructField[orgSchemaFields.length];

        for (int i = 0; i < orgSchemaFields.length; i++) {
            StructField field = orgSchemaFields[i];
            if (field.dataType() instanceof ArrayType && ((ArrayType) field.dataType()).elementType() instanceof StructType) {
                needsConversion = true;
                newSchemaFields[i] = new StructField(orgSchemaFields[i].name(), DataTypes.StringType, true, orgSchemaFields[i].metadata());
            } else {
                newSchemaFields[i] = field;
            }
        }

        if (!needsConversion) {
            return ds;
        }

        MapFunction<Row, Row> rowMapper = row -> {
            Object[] vals = new Object[row.length()];
            for (int i = 0; i < orgSchemaFields.length; i++) {
                Object val = row.get(i);
                StructField field = orgSchemaFields[i];
                if (field.dataType() instanceof ArrayType)
                    vals[i] = toRowsStr(row.getList(i), (ArrayType) field.dataType());
                else
                    vals[i] = val;
            }
            return RowFactory.create(vals);
        };

        StructType newSchema = ds.schema().copy(newSchemaFields);
        Dataset<Row> mappedDs = ds.map(rowMapper, RowEncoder.apply(newSchema));
        return mappedDs.sparkSession().createDataFrame(mappedDs.javaRDD(), newSchema);
    }

    public static Dataset<Row> unstringifyStructs(Dataset<Row> ds) {
        boolean needsConversion = false;
        StructField[] orgSchemaFields = ds.schema().fields();
        StructField[] newSchemaFields = new StructField[orgSchemaFields.length];

        for (int i = 0; i < orgSchemaFields.length; i++) {
            StructField field = orgSchemaFields[i];
            String[] optNameAndType = field.name().trim().split(":");
            if (optNameAndType.length == 2 && isArrayType(optNameAndType[1])) {
                String name = optNameAndType[0].trim();
                String type = optNameAndType[1].trim();
                needsConversion = true;
                ArrayType arrType = toArrayType(type);
                newSchemaFields[i] = new StructField(name, arrType, true, orgSchemaFields[i].metadata());
            } else {
                newSchemaFields[i] = field;
            }
        }

        if (!needsConversion) {
            return ds;
        }

        MapFunction<Row, Row> rowMapper = row -> {
            Object[] vals = new Object[row.length()];
            for (int i = 0; i < newSchemaFields.length; i++) {
                Object val = row.get(i);
                StructField field = newSchemaFields[i];
                if (field.dataType() instanceof ArrayType) {
                    if (val != null)
                        val = ((String) val).trim();
                    vals[i] = toRows((String) val, (ArrayType) field.dataType());
                } else
                    vals[i] = val;
            }
            return RowFactory.create(vals);
        };

        StructType newSchema = ds.schema().copy(newSchemaFields);
        Dataset<Row> mappedDs = ds.map(rowMapper, RowEncoder.apply(newSchema));
        return ds.sparkSession().createDataFrame(mappedDs.javaRDD(), newSchema);
    }

    public static boolean isArrayType(String type) {
        return schemaPattern.matcher(type).matches();
    }

    private static String toRowsStr(List<Row> rows, ArrayType schema) {
        String asStr = "";
        // Due to Java <=> Scala conversion bug, need to ensure underlying collection is not null
        // https://github.com/scala/scala/pull/4343/commits/a186f2be3f0b164f7edc7deefbec53deaa235290
        if (rows instanceof Wrappers.SeqWrapper && ((Wrappers.SeqWrapper) rows).underlying() != null) {
            StructField[] schemaFields = ((StructType) schema.elementType()).fields();
            for (Row row : rows) {
                assert row.size() == schemaFields.length : "# of fields (" + row.size() + ") != # of schema types (" + schemaFields.length + "): " + row;
                boolean isFirstField = true;
                asStr += "[";
                for (int i = 0; i < schemaFields.length; i++) {
                    if (isFirstField)
                        isFirstField = false;
                    else
                        asStr += ";";
                    asStr += toStr(row.get(i), schemaFields[i]);
                }
                asStr += "]";
            }
        }
        return asStr;
    }

    /**
     * Assuming correct schema and row structures.
     */
    static Row[] toRows(String rows, ArrayType schema) {
        StructType structType = (StructType) schema.elementType();
        StructField[] fieldSchemas = structType.fields();
        if (rows == null || rows.isEmpty()) {
            return new Row[0];
        }
        List<Row> results = new ArrayList<>();
        String[] rowStrs = splitRowsStr(rows);
        for (String row : rowStrs) {
            String[] fields = row.split(";");
            assert fields.length <= fieldSchemas.length : "# of fields (" + fields.length + ") != # of schema types (" + fieldSchemas.length + "): " + row;
            Object[] fieldVals = new Object[fieldSchemas.length];
            for (int i = 0; i < fields.length; i++)
                fieldVals[i] = (toObj(fields[i].trim(), fieldSchemas[i]));
            results.add(new GenericRowWithSchema(fieldVals, structType));
        }
        return results.toArray(new Row[results.size()]);
    }

    /**
     * Accepting Array of structs in format: col:Structs[a=Int;b=String;c=Date]
     * Note: not using colons as in a:Int as this breaks global split by ":"
     */
    private static ArrayType toArrayType(String type) {
        Matcher matcher = schemaPattern.matcher(type);
        if (matcher.find()) {
            Stream<String[]> subtypes = Arrays.stream(matcher.group(1).split(";")).map(x -> x.trim().split("="));
            Stream<StructField> fields = subtypes.map(x -> DataTypes.createStructField(x[0].trim(), toSimpleDataType(x[1].trim()), true));
            return DataTypes.createArrayType(DataTypes.createStructType(fields.collect(Collectors.toList())));
        } else throw new IllegalArgumentException("Invalid ArrayType: " + type + ", expecting: " + schemaPatternStr);
    }

    private static String toStr(Object o, StructField schema) {
        DataType type = schema.dataType();
        try {
            if (o == null)
                return "";
            else if (type instanceof IntegerType || type instanceof DoubleType || type instanceof DecimalType || type instanceof StringType)
                return o.toString();
            else if (type instanceof DateType)
                return DateFormatter.formatStandard.print(((java.sql.Date) o).getTime());
            else if (type instanceof TimestampType)
                return DateFormatter.formatStandard.print(((java.sql.Timestamp) o).getTime());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to map value " + o + " to type " + schema + ": " + e.getMessage(), e);
        }
        throw new IllegalArgumentException("Unsupported Structs type " + type + ", for value " + o + ", for schema " + schema);
    }

    private static Object toObj(String s, StructField schema) {
        DataType type = schema.dataType();
        try {
            if (s == null || s.isEmpty())
                return null;
            else if (type instanceof IntegerType)
                return Integer.valueOf(s);
            else if (type instanceof DoubleType)
                return Double.valueOf(s);
            else if (type instanceof DecimalType)
                return new BigDecimal(s);
            else if (type instanceof StringType)
                return s;
            else if (type instanceof DateType)
                return new java.sql.Date(DateFormatter.formatStandard.parseDateTime(s).getMillis());
            else if (type instanceof TimestampType)
                return new java.sql.Timestamp(DateFormatter.formatStandard.parseDateTime(s).getMillis());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to map string " + s + " to type " + schema + ": " + e.getMessage(), e);
        }
        throw new IllegalArgumentException("Unsupported Structs type " + type + ", for string " + s + ", for schema " + schema);
    }

    private static DataType toSimpleDataType(String type) {
        switch (type) {
            case "Int":
                return DataTypes.IntegerType;
            case "Date":
                return DataTypes.DateType;
            case "Timestamp":
                return DataTypes.TimestampType;
            case "Double":
                return DataTypes.DoubleType;
            case "Decimal":
                return DataTypes.createDecimalType(10, 2);
            case "String":
                return DataTypes.StringType;
            default:
                throw new IllegalArgumentException("Simple Type not supported: " + type);
        }
    }

    private static String toSimpleDataType(DataType type) {
        if (type instanceof IntegerType)
            return "Int";
        else if (type instanceof DateType)
            return "Date";
        else if (type instanceof TimestampType)
            return "Timestamp";
        else if (type instanceof DoubleType)
            return "Double";
        else if (type instanceof DecimalType)
            return "Decimal";
        else if (type instanceof StringType)
            return "String";
        else
            throw new IllegalArgumentException("Simple Type not supported: " + type);
    }

    /**
     * Split up any variations of:
     * " [ a; b; c ] [d;e;f][;;;] "-> [" a; b; c "; "d;e;f"; ";;;"]
     */
    private static String[] splitRowsStr(String rows) {
        rows = rows.trim();
        assert rows.startsWith("[") && rows.endsWith("]") : "rows " + rows + " must be [...]*";
        rows = rows.substring(1, rows.length() - 1);
        String[] rowLines = rows.trim().split("\\]\\w*\\[");
        return Arrays.stream(rowLines).map(String::trim).toArray(String[]::new);
    }
}
