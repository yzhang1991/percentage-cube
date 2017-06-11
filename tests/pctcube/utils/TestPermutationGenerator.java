package pctcube.utils;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class TestPermutationGenerator {

    @Test
    public void testPermutationGenerator() {
        PermutationGenerator<Integer> pgen = new PermutationGenerator<>();
        int elemCount = 3;
        Integer[] elements = new Integer[elemCount];
        for (int i = 0; i < elemCount; i++) {
            elements[i] = i;
        }
        pgen.addElements(elements);
        ArrayList<Integer> expectedSequence =
                new ArrayList<>(Arrays.asList(0, 1, 2, 0, 2, 1, 1, 0, 2, 1, 2, 0, 2, 0, 1, 2, 1, 0));
        ArrayList<Integer> actualSequence = new ArrayList<>();
        for (ArrayList<Integer> permutation : pgen) {
            for (Integer elem : permutation) {
                actualSequence.add(elem);
            }
        }
        assertEquals(expectedSequence, actualSequence);
    }

}
