package au.com.octo.fitnesse.fixtures.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SlimMessageUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(SlimMessageUtils.class);

    public static String pass(String message) {
        return "pass: " + message;
    }

    public static String report(String message) {
        return "report: " + message;
    }

    public static String fail(String expected, String actual, String message) {
        return fail(message + "expected: " + expected + "\n actual: " + actual);
    }

    public static String fail(String message) {
        LOGGER.error(message);
        return "fail: " + message;
    }

    public static String fail(String expected, String actual) {
        return fail(expected, actual, "");
    }

    public static List<List<String>> passTable(String message, List<List<String>> originalTable) {
        return resultTable(pass(message), originalTable);
    }

    public static List<List<String>> failTable(String message, List<List<String>> originalTable) {
        return resultTable(fail(message), originalTable);
    }

    private static List<List<String>> resultTable(String message, List<List<String>> originalTable) {
        List<List<String>> result = new ArrayList<>();
        for (int i = 0; i < originalTable.size(); i++) {
            List<String> row = new ArrayList<>();
            for (int j = 0; j < originalTable.get(0).size(); j++) {
                row.add("");
            }
            result.add(row);
        }
        result.get(0).set(0, message);
        return result;
    }

}
