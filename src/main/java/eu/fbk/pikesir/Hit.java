package eu.fbk.pikesir;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.collect.Ordering;

public final class Hit implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String documentID;

    private final Map<String, Double> layerScores;

    private double aggregateScore;

    public Hit(final String documentID) {
        this.documentID = Objects.requireNonNull(documentID);
        this.layerScores = new HashMap<>();
        this.aggregateScore = 0.0;
    }

    public String getDocumentID() {
        return this.documentID;
    }

    public double getLayerScore(final String layer) {
        Objects.requireNonNull(layer);
        final Double layerScore = this.layerScores.get(layer);
        return layerScore == null ? 0.0 : layerScore;
    }

    public void setLayerScore(final String layer, final double score) {
        Objects.requireNonNull(layer);
        if (score == 0.0) {
            this.layerScores.remove(layer);
        } else {
            this.layerScores.put(layer, score);
        }
    }

    public double getAggregateScore() {
        return this.aggregateScore;
    }

    public void setAggregateScore(final double aggregateScore) {
        this.aggregateScore = aggregateScore;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Hit)) {
            return false;
        }
        final Hit other = (Hit) object;
        return this.documentID.equals(other.documentID);
    }

    @Override
    public int hashCode() {
        return this.documentID.hashCode();
    }

    public String toString(final boolean includeLayerScores) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.documentID);
        builder.append(':');
        builder.append(String.format("%.3f", this.aggregateScore));
        if (includeLayerScores) {
            builder.append(" (");
            String separator = "";
            for (final String layer : Ordering.natural().sortedCopy(this.layerScores.keySet())) {
                builder.append(separator);
                builder.append(layer);
                builder.append(':');
                builder.append(String.format("%.3f", this.layerScores.get(layer).doubleValue()));
                separator = " ";
            }
            builder.append(')');
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public static Ordering<Hit> comparator(@Nullable final String layer, final boolean higherFirst) {
        return new Ordering<Hit>() {

            @Override
            public int compare(final Hit left, final Hit right) {

                int result;
                if (layer == null) {
                    result = Double.compare(left.aggregateScore, right.aggregateScore);
                } else {
                    result = Double.compare(left.getLayerScore(layer), right.getLayerScore(layer));
                }

                if (result == 0) {
                    result = left.documentID.compareTo(right.documentID);
                }

                return higherFirst ? -result : result;
            }

        };
    }

}
