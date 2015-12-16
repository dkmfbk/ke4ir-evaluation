package eu.fbk.pikesir.lucene;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

public final class WeightAttributeImpl extends AttributeImpl implements WeightAttribute {

    private float squaredWeightSum;

    public WeightAttributeImpl() {
        this.squaredWeightSum = 0.0f;
    }

    @Override
    public float getSquaredWeightSum() {
        return this.squaredWeightSum;
    }

    @Override
    public void setSquaredWeightSum(final float squaredWeightSum) {
        this.squaredWeightSum = squaredWeightSum;
    }

    @Override
    public void clear() {
        this.squaredWeightSum = 0.0f;
    }

    @Override
    public void copyTo(final AttributeImpl target) {
        final WeightAttributeImpl t = (WeightAttributeImpl) target;
        t.setSquaredWeightSum(this.squaredWeightSum);
    }

    @Override
    public void reflectWith(final AttributeReflector reflector) {
        reflector.reflect(WeightAttributeImpl.class, "squaredWeightSum", this.squaredWeightSum);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof WeightAttributeImpl)) {
            return false;
        }
        final WeightAttributeImpl other = (WeightAttributeImpl) object;
        return this.squaredWeightSum == other.squaredWeightSum;
    }

    @Override
    public int hashCode() {
        return Float.hashCode(this.squaredWeightSum);
    }

}