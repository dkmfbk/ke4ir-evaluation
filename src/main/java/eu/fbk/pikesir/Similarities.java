package eu.fbk.pikesir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

public class Similarities {

    private static final ThreadLocal<Map<String, Double>> DOC_NORMS = new ThreadLocal<Map<String, Double>>();

    private static final ThreadLocal<TermVector> QUERY_TERMS = new ThreadLocal<TermVector>();

    public static void setDocNorms(final Map<String, Double> docNorms) {
        DOC_NORMS.set(docNorms); // FIXME
    }

    public static void setQueryTerms(final TermVector queryTerms) {
        QUERY_TERMS.set(queryTerms); // FIXME
    }

    public static Similarity createCompoundSimilarity(
            final Map<String, Similarity> fieldSimilarities, final Similarity defaultSimilarity) {
        return new CompoundSimilarity(fieldSimilarities, defaultSimilarity);
    }

    public static Similarity createTfIdfSimilarity(final boolean enableCoord,
            final boolean enableNorm) {
        return new TfIdfSimilarity(enableCoord, enableNorm);
    }

    public static Similarity createWeightSimilarity(final boolean enableIDF) {
        return new WeightSimilarity(enableIDF);
    }

    public static Similarity create(final Path root, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a splitter for parsing field names
        final Splitter splitter = Splitter.on(Pattern.compile("[\\s,;]+")).omitEmptyStrings()
                .trimResults();

        // Create a TfIdf similarity, if enabled
        Similarity tfidfSimilarity = null;
        Set<String> tfidfFields = ImmutableSet.of();
        if (Boolean.parseBoolean(properties.getProperty(prefix + "tfidf", "false"))) {
            final boolean enableCoord = Boolean.parseBoolean( //
                    properties.getProperty(prefix + "tfidf.coord", "false"));
            final boolean enableNorm = Boolean.parseBoolean( //
                    properties.getProperty(prefix + "tfidf.norm", "false"));
            tfidfFields = ImmutableSet.copyOf(splitter.split( //
                    properties.getProperty(prefix + "tfidf.fields", "")));
            tfidfSimilarity = createTfIdfSimilarity(enableCoord, enableNorm);
        }

        // Create a weight similarity, if enabled
        Similarity weightSimilarity = null;
        Set<String> weightFields = ImmutableSet.of();
        if (Boolean.parseBoolean(properties.getProperty(prefix + "weight", "false"))) {
            final boolean enableIDF = Boolean.parseBoolean( //
                    properties.getProperty(prefix + "weight.idf", "false"));
            weightFields = ImmutableSet.copyOf(splitter.split( //
                    properties.getProperty(prefix + "weight.fields", "")));
            weightSimilarity = createWeightSimilarity(enableIDF);
        }

        // Return one of the two similarities, if it is enough
        if (tfidfSimilarity == null) {
            if (weightSimilarity == null) {
                return new ClassicSimilarity();
            } else if (weightFields.isEmpty()) {
                return weightSimilarity;
            }
        } else if (weightSimilarity == null) {
            if (tfidfFields.isEmpty()) {
                return tfidfSimilarity;
            }
        }

        // Compose similarities otherwise
        final Map<String, Similarity> fieldSimilarities = Maps.newHashMap();
        for (final String field : tfidfFields) {
            fieldSimilarities.put(field, tfidfSimilarity);
        }
        for (final String field : weightFields) {
            fieldSimilarities.put(field, weightSimilarity);
        }
        final Similarity defaultSimilarity = tfidfSimilarity != null //
                && tfidfFields.isEmpty() ? tfidfSimilarity : weightSimilarity != null
                && weightFields.isEmpty() ? weightSimilarity //
                : new ClassicSimilarity();
        return createCompoundSimilarity(fieldSimilarities, defaultSimilarity);
    }

    private static class CompoundSimilarity extends Similarity {

        private final Map<String, Similarity> fieldSimilarities;

        private final Similarity defaultSimilarity;

        CompoundSimilarity(final Map<String, Similarity> fieldSimilarities,
                final Similarity defaultSimilarity) {
            this.fieldSimilarities = ImmutableMap.copyOf(fieldSimilarities);
            this.defaultSimilarity = Objects.requireNonNull(defaultSimilarity);
        }

        private Similarity getSimilarity(final String field) {
            final Similarity similarity = this.fieldSimilarities.get(field);
            return similarity != null ? similarity : this.defaultSimilarity;
        }

        @Override
        public long computeNorm(final FieldInvertState state) {
            return getSimilarity(state.getName()).computeNorm(state);
        }

        @Override
        public float queryNorm(final float sumOfSquaredWeights) {
            return (float) (1.0 / Math.sqrt(sumOfSquaredWeights));
        }

        @Override
        public SimWeight computeWeight(final CollectionStatistics collectionStats,
                final TermStatistics... termStats) {
            final String field = collectionStats.field();
            final Similarity similarity = getSimilarity(field);
            final SimWeight weight = similarity.computeWeight(collectionStats, termStats);
            return new SimWeightWrapper(weight, similarity);
        }

        @Override
        public SimScorer simScorer(final SimWeight weight, final LeafReaderContext context)
                throws IOException {
            final SimWeightWrapper wrapper = (SimWeightWrapper) weight;
            return wrapper.similarity.simScorer(wrapper.weight, context);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(default: " + this.defaultSimilarity
                    + ", fields: "
                    + Joiner.on(", ").withKeyValueSeparator("=").join(this.fieldSimilarities);
        }

        private static final class SimWeightWrapper extends SimWeight {

            final SimWeight weight;

            final Similarity similarity;

            SimWeightWrapper(final SimWeight weight, final Similarity similarity) {
                this.weight = weight;
                this.similarity = similarity;
            }

            @Override
            public float getValueForNormalization() {
                return this.weight.getValueForNormalization();
            }

            @Override
            public void normalize(final float queryNorm, final float boost) {
                this.weight.normalize(queryNorm, boost);
            }

        }

    }

    private static class TfIdfSimilarity extends ClassicSimilarity {

        private final boolean enableCoord;

        private final boolean enableNorm;

        TfIdfSimilarity(final boolean enableCoord, final boolean enableNorm) {
            this.enableCoord = enableCoord;
            this.enableNorm = enableNorm;
        }

        @Override
        public float coord(final int overlap, final int maxOverlap) {
            return this.enableCoord ? super.coord(overlap, maxOverlap) : 1.0f;
        }

        @Override
        public float lengthNorm(final FieldInvertState state) {
            return this.enableNorm ? super.lengthNorm(state) : 1.0f;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(coord: " + this.enableCoord + ", norm: "
                    + this.enableNorm + ")";
        }

    }

    private static final class WeightSimilarity extends Similarity {

        private final boolean enableIDF;

        WeightSimilarity(final boolean enableIDF) {
            this.enableIDF = enableIDF;
        }

        @Override
        public float coord(final int overlap, final int maxOverlap) {
            return 1.0f;
        }

        @Override
        public long computeNorm(final FieldInvertState state) {
            Double norm = null;
            final Map<String, Double> norms = DOC_NORMS.get();
            if (norms != null) {
                norm = norms.get(state.getName());
            }
            return norm == null ? 0 : Double.doubleToLongBits(norm);
        }

        @Override
        public float queryNorm(final float sumOfSquaredWeights) {
            return 1.0f;
            // return (float) (1.0 / Math.sqrt(sumOfSquaredWeights));
        }

        @Override
        public SimWeight computeWeight(final CollectionStatistics collectionStats,
                final TermStatistics... termStats) {

            final List<String> tokens = Lists.newArrayListWithCapacity(termStats.length);
            for (final TermStatistics stat : termStats) {
                tokens.add(stat.term().utf8ToString());
            }

            Explanation idfExplanation = null;
            if (this.enableIDF) {
                float idf = 0.0f;
                final long maxDoc = collectionStats.maxDoc();
                final List<Explanation> subs = Lists.newArrayListWithCapacity(termStats.length);
                for (final TermStatistics stat : termStats) {
                    final long df = stat.docFreq();
                    final float termIdf = idf(df, maxDoc);
                    subs.add(Explanation.match(termIdf, "idf(docFreq=" + df //
                            + ", maxDocs=" + maxDoc + ")"));
                    idf += termIdf;
                }
                idfExplanation = subs.size() == 1 ? subs.get(0) : Explanation.match(idf,
                        "idf(), sum of:", subs);
            }

            return new CustomSimWeight(collectionStats.field(), tokens, idfExplanation);
        }

        @Override
        public SimScorer simScorer(final SimWeight stats, final LeafReaderContext context)
                throws IOException {
            final CustomSimWeight idfstats = (CustomSimWeight) stats;
            return new CustomSimScorer(idfstats, context);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(idf: " + this.enableIDF + ")";
        }

        private static float tf(final float freq) {
            return (float) Math.sqrt(freq);
        }

        private static float idf(final long docFreq, final long numDocs) {
            return (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0);
        }

        private static final class CustomSimWeight extends SimWeight {

            final String field;

            final List<String> tokens;

            @Nullable
            final Explanation idf;

            float queryNorm;

            float boost;

            float queryWeight;

            float value;

            CustomSimWeight(final String field, final List<String> tokens, final Explanation idf) {
                this.field = field;
                this.tokens = tokens;
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
                this.queryWeight = queryNorm * boost
                        * (this.idf == null ? 1.0f : this.idf.getValue());
                this.value = this.queryWeight * (this.idf == null ? 1.0f : this.idf.getValue());
            }

        }

        private static final class CustomSimScorer extends SimScorer {

            final LeafReaderContext context;

            final CustomSimWeight stats;

            CustomSimScorer(final CustomSimWeight stats, final LeafReaderContext context)
                    throws IOException {
                this.context = context;
                this.stats = stats;
            }

            @Override
            public float score(final int doc, final float freq) {

                final TermVector queryTerms = QUERY_TERMS.get();
                final Term queryTerm = queryTerms.getTerm(Field.forID(this.stats.field),
                        this.stats.tokens.get(0));

                float weight = 1.0f;
                float norm = 1.0f;
                try {
                    norm = (float) Double.longBitsToDouble(this.context.reader()
                            .getNormValues(this.stats.field).get(doc));

                    final PostingsEnum pe = this.context.reader().postings(
                            new org.apache.lucene.index.Term(this.stats.field,
                                    this.stats.tokens.get(0)), PostingsEnum.PAYLOADS);
                    pe.advance(doc);
                    pe.nextPosition();
                    final BytesRef payload = pe.getPayload();
                    if (payload != null) {
                        weight = Float.intBitsToFloat(Ints.fromByteArray(BytesRef
                                .deepCopyOf(payload).bytes));
                    }
                } catch (final IOException ex) {
                    Throwables.propagate(ex);
                }

                //  System.out.println(weight + " - " + Math.pow(1.0 / norm, 2.0));

                // return tf(weight) * this.stats.value  * norm * (float) queryTerm.getWeight();
                return tf(weight) * this.stats.value * (float) queryTerm.getWeight(); // compute tf*idf
            }

            @Override
            public float computeSlopFactor(final int distance) {
                return 1.0f / (distance + 1); // unused
            }

            @Override
            public float computePayloadFactor(final int doc, final int start, final int end,
                    final BytesRef payload) {
                return 1.0f; // never called (!)
            }

            @Override
            public Explanation explain(final int doc, final Explanation freq) {

                final List<Explanation> subs = new ArrayList<>();
                final Explanation boostExpl = Explanation.match(this.stats.boost, "boost");
                if (this.stats.boost != 1.0f) {
                    subs.add(boostExpl);
                }
                subs.add(this.stats.idf);
                final Explanation queryNormExpl = Explanation.match(this.stats.queryNorm,
                        "queryNorm");
                subs.add(queryNormExpl);
                final Explanation queryExpl = Explanation.match(boostExpl.getValue()
                        * this.stats.idf.getValue() * queryNormExpl.getValue(),
                        "queryWeight, product of:", subs);

                final Explanation tfExplanation = Explanation.match(tf(freq.getValue()),
                        "tf(freq=" + freq.getValue() + "), with freq of:", freq);
                final Explanation fieldNormExpl = Explanation.match(1.0f, "fieldNorm(doc=" + doc
                        + ")");
                final Explanation fieldExpl = Explanation.match(tfExplanation.getValue()
                        * this.stats.idf.getValue() * fieldNormExpl.getValue(), "fieldWeight in "
                        + doc + ", product of:", tfExplanation, this.stats.idf, fieldNormExpl);

                if (queryExpl.getValue() == 1f) {
                    return fieldExpl;
                }
                return Explanation.match(queryExpl.getValue() * fieldExpl.getValue(), "score(doc="
                        + doc + ",freq=" + freq.getValue() + "), product of:", queryExpl,
                        fieldExpl);
            }

        }

    }

}
