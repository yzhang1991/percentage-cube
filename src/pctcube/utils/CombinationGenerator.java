package pctcube.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CombinationGenerator<T> implements Iterable<ArrayList<T>>, Iterator<ArrayList<T>> {

    public CombinationGenerator() { }

    public CombinationGenerator(T[] elements) {
        addElements(elements);
    }

    public void addElement(T element) {
        m_elements.add(element);
        reset();
    }

    public void addElements(T[] elements) {
        if (elements != null) {
            for (T element : elements) {
                addElement(element);
            }
        }
    }

    public void reset(int numOfSelectedElements) {
        m_numOfSelectedItems = numOfSelectedElements;
    }

    public void reset() {
        reset(m_elements.size());
    }

    @Override
    public boolean hasNext() {

    }

    @Override
    public ArrayList<T> next() {

    }

    @Override
    public Iterator<ArrayList<T>> iterator() {
        return this;
    }

    private void swap(int a, int b) {
        Integer temp = m_positions.get(a);
        m_positions.set(a, m_positions.get(b));
        m_positions.set(b, temp);
    }

    private ArrayList<T> getCurrentPermutation() {
        ArrayList<T> retval = new ArrayList<>();
        for (int i = 0; i < m_numOfSelectedItems; i++) {
            retval.add(m_elements.get(m_positions.get(i)));
        }
        return retval;
    }

    public String getCurrentCombinationString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < m_numOfSelectedItems; i++) {
            builder.append(m_positions.get(i));
        }
        return builder.toString();
    }

    private List<T> m_elements = new ArrayList<>();
    private List<T> m_selectedElements;
    private int m_numOfSelectedItems = 0;
}
