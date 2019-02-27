package au.com.octo.fitnesse.domain;


public class Column {

    private String name;
    private Transformation transformation;

    public Column(String name, Transformation transformation) {
        this.name = name;
        this.transformation = transformation;
    }

    public Transformation getTransformation() {
        return transformation;
    }

    public String getName() {
        return name;
    }

}

