package eu.fbk.pikesir;

import java.util.Objects;

public final class Term implements Comparable<Term> {

    private final String field;

    private final String value;

    private final int frequency;

    private final double weight;

    private Term(final String field, final String value, final int frequency, final double weight) {
        this.field = field;
        this.value = value;
        this.frequency = frequency;
        this.weight = weight;
    }

    public static Term create(final String field, final String value) {
        return create(field, value, 1, 1.0);
    }

    public static Term create(final String field, final String value, final int frequency,
            final double weight) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(value);
        return new Term(field, value, frequency, weight);
    }

    public String getField() {
        return this.field;
    }

    public String getValue() {
        return this.value;
    }

    public int getFrequency() {
        return this.frequency;
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
        return this.field + ": " + this.value + "/" + this.frequency + "/"
                + String.format("%.2f", this.weight);
    }

}