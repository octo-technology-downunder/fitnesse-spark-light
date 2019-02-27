package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.Transformation;

public class ComparatorFactory {

    public static ValueComparator getComparator(Transformation transformation) {
        switch (transformation) {
            case TRIM:
                return new TrimTransformationComparator();
            case DOUBLE:
                return new DoubleTransformationComparator();
            case DOUBLE_EMPTY:
                return new DoubleEmptyTransformationComparator();
            case BLOB:
                return new BlobComparator();
            case UNKNOWN:
                return new RawValueComparator();
            default:
                throw new RuntimeException("Unknown transformation");
        }
    }
}
