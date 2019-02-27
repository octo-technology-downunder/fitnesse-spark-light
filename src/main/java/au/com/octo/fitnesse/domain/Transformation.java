package au.com.octo.fitnesse.domain;


public enum Transformation {

    TRIM("trim"), DOUBLE("double"), DOUBLE_EMPTY("doubleEmpty"), BLOB("blob"), UNKNOWN("unknown");

    private String value;

    Transformation(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Transformation getValueFromString(String text) {
        for (Transformation b : Transformation.values()) {
            if (b.value.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return UNKNOWN;
    }
}
