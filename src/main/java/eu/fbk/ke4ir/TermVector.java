package eu.fbk.ke4ir;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public final class TermVector implements Comparable<TermVector> {

    public static final TermVector EMPTY = new TermVector(ImmutableList.of());

    private static final String HIGHEST_TERM_VALUE = (char) 0xFFFF + "";

    private final List<Term> terms;

    private transient int hash;

    private transient Set<String> layers;

    private TermVector(final List<Term> terms) {
        this.terms = ImmutableList.copyOf(terms);
    }

    public boolean isEmpty() {
        return this.terms.isEmpty();
    }

    public int size() {
        return this.terms.size();
    }

    public Set<String> getLayers() {
        if (this.layers == null) {
            final Set<String> layers = Sets.newHashSet();
            for (final Term term : this.terms) {
                layers.add(term.getField());
            }
            this.layers = ImmutableSet.copyOf(layers);
        }
        return this.layers;
    }

    public List<Term> getTerms() {
        return this.terms;
    }

    public List<Term> getTerms(final String field) {
        final int start = -Collections.binarySearch(this.terms, Term.create(field, "")) - 1;
        int end = this.terms.size();
        if (start < this.terms.size()) {
            end = -Collections.binarySearch(this.terms, Term.create(field, HIGHEST_TERM_VALUE)) - 1;
        }
        return this.terms.subList(start, end);
    }

    @Nullable
    public Term getTerm(final String field, final String value) {
        final int index = Collections.binarySearch(this.terms, Term.create(field, value));
        return index >= 0 ? this.terms.get(index) : null;
    }

    public TermVector scale(final double factor) {
        final List<Term> newTerms = new ArrayList<>(this.terms.size());
        for (final Term term : this.terms) {
            newTerms.add(Term.create(term.getField(), term.getValue(), term.getFrequency(),
                    term.getWeight() * factor));
        }
        return new TermVector(newTerms);
    }

    public TermVector project(final Iterable<String> fields) {
        final Set<String> fieldSet = ImmutableSet.copyOf(fields);
        final List<Term> newTerms = new ArrayList<>(this.terms.size());
        for (final Term term : this.terms) {
            if (fieldSet.contains(term.getField())) {
                newTerms.add(term);
            }
        }
        return newTerms.size() == this.terms.size() ? this : new TermVector(newTerms);
    }

    public TermVector add(final TermVector vector) {
        final List<Term> terms1 = this.terms;
        final List<Term> terms2 = vector.terms;
        if (terms1.size() == 0) {
            return vector;
        } else if (terms2.size() == 0) {
            return this;
        }
        final List<Term> newTerms = new ArrayList<>(terms1.size() + terms2.size());
        int index2 = 0;
        for (final Term term1 : terms1) {
            while (index2 < terms2.size()) {
                final Term term2 = terms2.get(index2);
                final int compare = term1.compareTo(term2);
                if (compare == 0) {
                    newTerms.add(Term.create(term1.getField(), term1.getValue(),
                            term1.getFrequency() + term2.getFrequency(),
                            term1.getWeight() + term2.getWeight()));
                    ++index2;
                    break;
                } else if (compare > 0) {
                    newTerms.add(term2);
                    ++index2;
                } else {
                    newTerms.add(term1);
                    break;
                }
            }
        }
        newTerms.addAll(terms2.subList(index2, terms2.size()));
        return new TermVector(newTerms);
    }

    public TermVector product(final TermVector vector) {
        final List<Term> terms1 = this.terms;
        final List<Term> terms2 = vector.terms;
        if (terms1.size() == 0 || terms2.size() == 0) {
            return EMPTY;
        } else if (terms1.size() > terms2.size()) {
            return vector.product(this);
        }
        final List<Term> newTerms = new ArrayList<>(terms1.size());
        int index2 = 0;
        for (final Term term1 : terms1) {
            while (index2 < terms2.size()) {
                final Term term2 = terms2.get(index2);
                final int compare = term1.compareTo(term2);
                if (compare == 0) {
                    newTerms.add(Term.create(term1.getField(), term1.getValue(),
                            term1.getFrequency(), term1.getWeight() * term2.getWeight()));
                    ++index2;
                    break;
                } else if (compare > 0) {
                    ++index2;
                } else {
                    break;
                }
            }
        }
        return new TermVector(newTerms);
    }

    @Override
    public int compareTo(final TermVector other) {
        for (int i = 0; i < this.terms.size(); ++i) {
            if (i >= other.terms.size()) {
                return 1;
            }
            final Term thisTerm = this.terms.get(i);
            final Term otherTerm = other.terms.get(i);
            int result = thisTerm.compareTo(otherTerm);
            if (result != 0) {
                return result;
            }
            result = Double.compare(thisTerm.getWeight(), otherTerm.getWeight());
            if (result != 0) {
                return result;
            }
        }
        return this.terms.size() - other.terms.size();
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof TermVector)) {
            return false;
        }
        final TermVector other = (TermVector) object;
        if (this.terms.size() != other.terms.size()) {
            return false;
        }
        for (int i = 0; i < this.terms.size(); ++i) {
            final Term thisTerm = this.terms.get(i);
            final Term otherTerm = other.terms.get(i);
            if (!thisTerm.equals(otherTerm) || thisTerm.getWeight() != otherTerm.getWeight()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (this.hash == 0) {
            this.hash = this.terms.hashCode();
        }
        return this.hash;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("[");
        String field = null;
        String separator = "";
        for (final Term term : this.terms) {
            if (field == null || !field.equals(term.getField())) {
                builder.append(separator).append(term.getField()).append(":");
                separator = "; ";
                field = term.getField();
            }
            builder.append(' ').append(term.getValue());
            if (term.getWeight() != 1.0) {
                builder.append("/").append(String.format("%.2f", term.getWeight()));
            }
        }
        builder.append("]");
        return builder.toString();
    }

    public static void write(final Writer writer, final Map<String, TermVector> vectors)
            throws IOException {

        for (final Map.Entry<String, TermVector> entry : vectors.entrySet()) {
            final String id = entry.getKey();
            for (final Term term : entry.getValue().terms) {
                writeEscaped(writer, id);
                writer.write('\t');
                writeEscaped(writer, term.getField());
                writer.write('\t');
                writeEscaped(writer, term.getValue());
                writer.write('\t');
                writeEscaped(writer, Integer.toString(term.getFrequency()));
                writer.write('\t');
                writeEscaped(writer, Double.toString(term.getWeight()));
                writer.write('\n');
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Map<String, TermVector> read(final Reader reader) throws IOException {

        final Map<String, Object> map = new HashMap<>();

        final StringBuilder sb = new StringBuilder();
        while (true) {
            final String id = readEscaped(reader, sb);
            if (id.isEmpty()) {
                break;
            }
            final String field = readEscaped(reader, sb);
            final String value = readEscaped(reader, sb);
            final int frequency = Integer.parseInt(readEscaped(reader, sb));
            final double weight = Double.parseDouble(readEscaped(reader, sb));
            final Term term = Term.create(field, value, frequency, weight);
            Builder builder = (Builder) map.get(id);
            if (builder == null) {
                builder = builder();
                map.put(id, builder);
            }
            builder.addTerm(term);
        }

        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            entry.setValue(((Builder) entry.getValue()).build());
        }

        return (Map) map;
    }

    private static void writeEscaped(final Writer writer, final String string) throws IOException {
        final int len = string.length();
        for (int i = 0; i < len; ++i) {
            final char c = string.charAt(i);
            if (c == '\t') {
                writer.write("\\t");
            } else if (c == '\n') {
                writer.write("\\n");
            } else if (c == '\r') {
                writer.write("\\r");
            } else if (c == '\\') {
                writer.write("\\\\");
            } else {
                writer.write(c);
            }
        }
    }

    private static String readEscaped(final Reader reader, final StringBuilder builder)
            throws IOException {
        builder.setLength(0);
        boolean escaping = false;
        while (true) {
            final int c = reader.read();
            if (c < 0 || c == '\n' || c == '\t') {
                return builder.toString();
            } else if (escaping) {
                if (c == 't') {
                    builder.append('\t');
                } else if (c == 'n') {
                    builder.append('\n');
                } else if (c == 'r') {
                    builder.append('\r');
                } else if (c == '\\') {
                    builder.append('\\');
                } else {
                    builder.append('\\').append((char) c);
                }
            } else if (c == '\\') {
                escaping = true;
            } else {
                builder.append((char) c);
            }
        }
    }

    public static Builder builder() {
        return new Builder(new ArrayList<>());
    }

    public static Builder builder(final TermVector document) {
        return new Builder(new ArrayList<>(document.terms));
    }

    public final static class Builder {

        private final List<Term> terms;

        Builder(final List<Term> terms) {
            this.terms = terms;
        }

        public Builder addTerm(final String field, final String value, final int frequency,
                final double weight) {
            return addTerm(Term.create(field, value, frequency, weight));
        }

        public Builder addTerm(final String field, final String value) {
            return addTerm(Term.create(field, value));
        }

        public Builder addTerm(final Term term) {
            final int index = Collections.binarySearch(this.terms, term);
            if (index >= 0) {
                final Term oldTerm = this.terms.get(index);
                final Term newTerm = Term.create(oldTerm.getField(), oldTerm.getValue(),
                        oldTerm.getFrequency() + term.getFrequency(),
                        oldTerm.getWeight() + term.getWeight());
                this.terms.set(index, newTerm);
            } else {
                this.terms.add(-index - 1, term);
            }
            return this;
        }

        public Builder addTerms(final Iterable<Term> terms) {
            for (final Term term : terms) {
                addTerm(term);
            }
            return this;
        }

        public TermVector build() {
            return new TermVector(ImmutableList.copyOf(this.terms));
        }

    }

}
