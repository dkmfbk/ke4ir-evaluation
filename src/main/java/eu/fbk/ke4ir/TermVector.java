package eu.fbk.ke4ir;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

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
            end = -Collections.binarySearch(this.terms, Term.create(field, HIGHEST_TERM_VALUE))
                    - 1;
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

    public void write(final Document document) {

        try {
            final Set<String> fields = Sets.newHashSet();
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(bos);

            dos.writeUTF(Joiner.on('|').join(Ordering.natural().sortedCopy(getLayers())));

            for (final Term term : this.getTerms()) {

                for (int i = 0; i < term.getFrequency(); ++i) {
                    document.add(new TextField(term.getField(), term.getValue(), Store.YES));
                }

                fields.add(term.getField());
                dos.writeInt(term.getFrequency());
                dos.writeFloat((float) term.getWeight());
            }

            dos.close();

            document.add(new StoredField("_data", bos.toByteArray()));

        } catch (final IOException ex) {
            Throwables.propagate(ex);
        }
    }

    public void write(final Writer writer, final String id) throws IOException {

        for (final Term term : getTerms()) {
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

    public static TermVector read(final Document document) {

        try {
            final List<Term> augmentedTerms = Lists.newArrayList();

            final BytesRef data = document.getBinaryValue("_data");
            final DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(data.bytes, data.offset, data.length));

            final Set<String> fields = ImmutableSet.copyOf(Splitter.on('|').split(in.readUTF()));

            final Set<Term> terms = Sets.newHashSet();
            for (final IndexableField field : document.getFields()) {
                if (fields.contains(field.name())) {
                    terms.add(Term.create(field.name(), field.stringValue()));
                }
            }

            for (final Term term : Ordering.natural().sortedCopy(terms)) {
                final int frequency = in.readInt();
                final float weight = in.readFloat();
                augmentedTerms
                        .add(Term.create(term.getField(), term.getValue(), frequency, weight));
            }

            return builder().addTerms(augmentedTerms).build();

        } catch (final IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Nullable
    public static Entry<String, TermVector> read(final BufferedReader reader) throws IOException {

        final Builder builder = builder();
        String id = null;

        final StringBuilder sb = new StringBuilder();
        while (true) {
            reader.mark(256);
            final String newId = readEscaped(reader, sb);
            if (newId.isEmpty() || id != null && !id.equals(newId)) {
                reader.reset();
                break;
            }
            id = newId;
            final String field = readEscaped(reader, sb);
            final String value = readEscaped(reader, sb);
            final int frequency = Integer.parseInt(readEscaped(reader, sb));
            final double weight = Double.parseDouble(readEscaped(reader, sb));
            builder.addTerm(field, value, frequency, weight);
        }

        return id == null ? null : new SimpleEntry<>(id, builder.build());
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
