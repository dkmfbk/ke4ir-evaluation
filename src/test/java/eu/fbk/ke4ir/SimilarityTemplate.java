package eu.fbk.ke4ir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

public class SimilarityTemplate extends Similarity {

    private static final float[] NORM_TABLE = new float[256];

    protected boolean discountOverlaps = true;

    static {
        for (int i = 0; i < 256; i++) {
            NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte) i);
        }
    }

    @Override
    public float coord(final int overlap, final int maxOverlap) {
        return overlap / (float) maxOverlap;
    }

    @Override
    public float queryNorm(final float sumOfSquaredWeights) {
        return (float) (1.0 / Math.sqrt(sumOfSquaredWeights));
    }

    @Override
    public final long computeNorm(final FieldInvertState state) {
        final float normValue = lengthNorm(state);
        return encodeNormValue(normValue);
    }

    @Override
    public final SimWeight computeWeight(final CollectionStatistics collectionStats,
            final TermStatistics... termStats) {
        final Explanation idf = termStats.length == 1 ? idfExplain(collectionStats, termStats[0])
                : idfExplain(collectionStats, termStats);
        return new IDFStats(collectionStats.field(), idf);
    }

    @Override
    public final SimScorer simScorer(final SimWeight stats, final LeafReaderContext context)
            throws IOException {
        final IDFStats idfstats = (IDFStats) stats;
        return new TFIDFSimScorer(idfstats, context.reader().getNormValues(idfstats.field));
    }

    public float tf(final float freq) {
        return (float) Math.sqrt(freq);
    }

    public Explanation idfExplain(final CollectionStatistics collectionStats,
            final TermStatistics termStats) {
        final long df = termStats.docFreq();
        final long max = collectionStats.maxDoc();
        final float idf = idf(df, max);
        return Explanation.match(idf, "idf(docFreq=" + df + ", maxDocs=" + max + ")");
    }

    public Explanation idfExplain(final CollectionStatistics collectionStats,
            final TermStatistics termStats[]) {
        final long max = collectionStats.maxDoc();
        float idf = 0.0f;
        final List<Explanation> subs = new ArrayList<>();
        for (final TermStatistics stat : termStats) {
            final long df = stat.docFreq();
            final float termIdf = idf(df, max);
            subs.add(Explanation.match(termIdf, "idf(docFreq=" + df + ", maxDocs=" + max + ")"));
            idf += termIdf;
        }
        return Explanation.match(idf, "idf(), sum of:", subs);
    }

    public float idf(final long docFreq, final long numDocs) {
        return (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0);
    }

    public float lengthNorm(final FieldInvertState state) {
        final int numTerms;
        if (this.discountOverlaps) {
            numTerms = state.getLength() - state.getNumOverlap();
        } else {
            numTerms = state.getLength();
        }
        return state.getBoost() * (float) (1.0 / Math.sqrt(numTerms));
    }

    public final float decodeNormValue(final long norm) {
        return NORM_TABLE[(int) (norm & 0xFF)]; // & 0xFF maps negative bytes to positive above 127
    }

    public final long encodeNormValue(final float f) {
        return SmallFloat.floatToByte315(f);
    }

    public float sloppyFreq(final int distance) {
        return 1.0f / (distance + 1);
    }

    public float scorePayload(final int doc, final int start, final int end, final BytesRef payload) {
        return 1;
    }

    private final class TFIDFSimScorer extends SimScorer {

        private final IDFStats stats;
        private final float weightValue;
        private final NumericDocValues norms;

        TFIDFSimScorer(final IDFStats stats, final NumericDocValues norms) throws IOException {
            this.stats = stats;
            this.weightValue = stats.value;
            this.norms = norms;
        }

        @Override
        public float score(final int doc, final float freq) {
            final float raw = tf(freq) * this.weightValue; // compute tf(f)*weight
            return this.norms == null ? raw : raw * decodeNormValue(this.norms.get(doc)); // normalize for field
        }

        @Override
        public float computeSlopFactor(final int distance) {
            return sloppyFreq(distance);
        }

        @Override
        public float computePayloadFactor(final int doc, final int start, final int end,
                final BytesRef payload) {
            return scorePayload(doc, start, end, payload);
        }

        @Override
        public Explanation explain(final int doc, final Explanation freq) {
            return explainScore(doc, freq, this.stats, this.norms);
        }
    }

    private static class IDFStats extends SimWeight {

        private final String field;
        private final Explanation idf;
        private float queryNorm;
        private float boost;
        private float queryWeight;
        private float value;

        public IDFStats(final String field, final Explanation idf) {
            this.field = field;
            this.idf = idf;
            normalize(1f, 1f);
        }

        @Override
        public float getValueForNormalization() {
            return this.queryWeight * this.queryWeight; // sum of squared weights
        }

        @Override
        public void normalize(final float queryNorm, final float boost) {
            this.boost = boost;
            this.queryNorm = queryNorm;
            this.queryWeight = queryNorm * boost * this.idf.getValue();
            this.value = this.queryWeight * this.idf.getValue();
        }
    }

    private Explanation explainQuery(final IDFStats stats) {
        final List<Explanation> subs = new ArrayList<>();

        final Explanation boostExpl = Explanation.match(stats.boost, "boost");
        if (stats.boost != 1.0f) {
            subs.add(boostExpl);
        }
        subs.add(stats.idf);

        final Explanation queryNormExpl = Explanation.match(stats.queryNorm, "queryNorm");
        subs.add(queryNormExpl);

        return Explanation.match(
                boostExpl.getValue() * stats.idf.getValue() * queryNormExpl.getValue(),
                "queryWeight, product of:", subs);
    }

    private Explanation explainField(final int doc, final Explanation freq, final IDFStats stats,
            final NumericDocValues norms) {
        final Explanation tfExplanation = Explanation.match(tf(freq.getValue()),
                "tf(freq=" + freq.getValue() + "), with freq of:", freq);
        final Explanation fieldNormExpl = Explanation.match(
                norms != null ? decodeNormValue(norms.get(doc)) : 1.0f, "fieldNorm(doc=" + doc
                        + ")");

        return Explanation
                .match(tfExplanation.getValue() * stats.idf.getValue() * fieldNormExpl.getValue(),
                        "fieldWeight in " + doc + ", product of:", tfExplanation, stats.idf,
                        fieldNormExpl);
    }

    private Explanation explainScore(final int doc, final Explanation freq, final IDFStats stats,
            final NumericDocValues norms) {
        final Explanation queryExpl = explainQuery(stats);
        final Explanation fieldExpl = explainField(doc, freq, stats, norms);
        if (queryExpl.getValue() == 1f) {
            return fieldExpl;
        }
        return Explanation.match(queryExpl.getValue() * fieldExpl.getValue(), "score(doc=" + doc
                + ",freq=" + freq.getValue() + "), product of:", queryExpl, fieldExpl);
    }

}
