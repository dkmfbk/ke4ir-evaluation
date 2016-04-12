package eu.fbk.ke4ir;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import eu.fbk.ke4ir.TermVector;

public class TermVectorTest {

    @Test
    public void test() {
        final TermVector v1 = TermVector.builder().addTerm("textual", "stem1", 1, 0.5)
                .addTerm("uri", "concept1").build();
        final TermVector v2 = TermVector.builder().addTerm("textual", "stem1", 1, 0.5)
                .addTerm("uri", "concept1", 1, 0.5).addTerm("uri", "concept2").build();
        System.out.println("Scale: " + v1.scale(2.0));
        System.out.println("Sum: " + v1.add(v2));
        System.out.println("Product: " + v1.product(v2));
        System.out.println(v2.getTerms("textual"));
        System.out.println(v2.getTerms("uri"));
        System.out.println(v2.project(ImmutableList.of("uri")));
    }

}
