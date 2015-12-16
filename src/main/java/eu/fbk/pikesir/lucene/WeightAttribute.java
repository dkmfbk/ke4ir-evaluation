package eu.fbk.pikesir.lucene;

import org.apache.lucene.util.Attribute;

public interface WeightAttribute extends Attribute {

    float getSquaredWeightSum();

    void setSquaredWeightSum(final float squaredWeightSum);

    default void addWeight(final float weight) {
        setSquaredWeightSum(getSquaredWeightSum() + weight * weight);
    }

}
