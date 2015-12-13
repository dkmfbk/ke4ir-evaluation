package eu.fbk.pikesir;

import java.util.Objects;

import com.google.common.base.Preconditions;

public final class Term implements Comparable<Term> {

    private final Field field;

    private final String value;

    private final double weight;

    private Term(final Field field, final String value, final double weight) {
        this.field = field;
        this.value = value;
        this.weight = weight;
    }

    public static Term create(final Field field, final String value) {
        return create(field, value, 1.0);
    }

    public static Term create(final Field field, final String value, final double weight) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(value);
        Preconditions.checkArgument(weight != 0.0);
        return new Term(field, value, weight);
    }

    public Field getField() {
        return this.field;
    }

    public String getValue() {
        return this.value;
    }

    public double getWeight() {
        return this.weight;
    }

    @Override
    public int compareTo(final Term other) {
        int result = this.field.compareTo(other.field);
        if (result == 0) {
            result = this.value.compareTo(other.value);
        }
        return result;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Term)) {
            return false;
        }
        final Term other = (Term) object;
        return this.field == other.field && this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field, this.value);
    }

    @Override
    public String toString() {
        return this.field + ": " + this.value
                + (this.weight == 1.0 ? "" : "/" + String.format("%.2f", this.weight));
    }

}