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
        for (int i = 0; i < elemCount; i++) {
            cgen.addElement(i);
        }
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
