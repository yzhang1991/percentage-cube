package pctcube.utils;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class TestCombinationGenerator {

    @Test
    public void test() {
        ArrayList<Integer> expectedSequence =
                new ArrayList<>(Arrays.asList(0, 1, 0, 2, 0, 3, 1, 2, 1, 3, 2, 3));
        CombinationGenerator<Integer> cgen = new CombinationGenerator<>();
        int elemCount = 4;
        Integer[] elements = new Integer[elemCount];
        for (int i = 0; i < elemCount; i++) {
            elements[i] = i;
        }
        cgen.addElements(elements);
        ArrayList<Integer> actualSequence = new ArrayList<>();
        cgen.setNumOfElementsToSelect(2);

        for (ArrayList<Integer> permutation : cgen) {
            for (Integer elem : permutation) {
                actualSequence.add(elem);
            }
        }
        assertEquals(expectedSequence, actualSequence);
    }

}
