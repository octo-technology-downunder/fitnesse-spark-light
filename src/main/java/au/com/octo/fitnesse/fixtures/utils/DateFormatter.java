package au.com.octo.fitnesse.fixtures.utils;

import org.joda.time.format.DateTimeFormatter;

import static org.joda.time.format.DateTimeFormat.forPattern;

public class DateFormatter {

    public static final DateTimeFormatter formatStandard = forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

}
