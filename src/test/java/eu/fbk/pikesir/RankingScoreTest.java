package eu.fbk.pikesir;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import eu.fbk.pikesir.RankingScore;

public class RankingScoreTest {

    @Test
    public void test() {
        final RankingScore score = RankingScore.evaluate(ImmutableList.of("x", "a", "b", "c"),
                ImmutableSet.of("a", "c", "d"));
        System.out.println(score);
    }

}
