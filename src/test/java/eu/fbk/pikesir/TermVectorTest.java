package eu.fbk.pikesir;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

public class TermVectorTest {

    @Test
    public void test() {
        final TermVector v1 = TermVector.builder().addTerm(Field.STEM, "stem1", 0.5)
                .addTerm(Field.CONCEPT, "concept1").build();
        final TermVector v2 = TermVector.builder().addTerm(Field.STEM, "stem1", 0.5)
                .addTerm(Field.CONCEPT, "concept1", 0.5).addTerm(Field.CONCEPT, "concept2")
                .build();
        System.out.println("Scale: " + v1.scale(2.0));
        System.out.println("Sum: " + v1.add(v2));
        System.out.println("Product: " + v1.product(v2));
        System.out.println(v2.getTerms(Field.STEM));
        System.out.println(v2.getTerms(Field.CONCEPT));
        System.out.println(v2.project(ImmutableList.of(Field.CONCEPT)));
    }

}
