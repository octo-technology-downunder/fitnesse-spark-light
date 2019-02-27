package au.com.octo.fitnesse.domain;

public class CellComparisonResult {

    private boolean error;
    private String message;

    public CellComparisonResult(boolean error, String message) {
        this.error = error;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public boolean isError() {
        return error;
    }
}
