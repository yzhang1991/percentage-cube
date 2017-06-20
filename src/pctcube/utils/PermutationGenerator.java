package pctcube.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PermutationGenerator<T> implements Iterable<ArrayList<T>>, Iterator<ArrayList<T>> {

    public PermutationGenerator() { }

    public PermutationGenerator(List<T> elements) {
        addElements(elements);
    }

    public void addElement(T element) {
        m_elements.add(element);
        m_positions.add(m_positions.size());

        reset();
    }

    public void addElements(List<T> elements) {
        if (elements != null) {
            for (T element : elements) {
                addElement(element);
            }
        }
    }

    public void reset() {
        m_beforeFirst = true;
    }

    @Override
    public boolean hasNext() {
        if (m_beforeFirst) {
            return true;
        }
        if (m_positions.size() <= 1) {
            return false;
        }
        int i = m_positions.size() - 2;
        while (i >= 0 && m_positions.get(i) > m_positions.get(i+1)) {
            i--;
        }
        if (i < 0) {
            return false;
        }
        return true;
    }

    @Override
    public ArrayList<T> next() {
        if (m_beforeFirst) {
            for (int i = 0; i < m_positions.size(); i++) {
                m_positions.set(i, i);
            }
            m_beforeFirst = false;
            return getCurrentPermutation();
        }
        if (m_positions.size() <= 1) {
            throw new NoSuchElementException();
        }
        int i = m_positions.size() - 2;
        while (i >= 0 && m_positions.get(i) > m_positions.get(i+1)) {
            i--;
        }
        if (i < 0) {
            throw new NoSuchElementException();
        }
        int j = m_positions.size() - 1;
        while (j > i && m_positions.get(j) < m_positions.get(i)) {
            j--;
        }
        swap(i, j);
        int k;
        for (j = i + 1, k = m_positions.size() - 1; j < k; j++, k--) {
            swap(j, k);
        }
        return getCurrentPermutation();
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
        for (int i = 0; i < m_positions.size(); i++) {
            retval.add(m_elements.get(m_positions.get(i)));
        }
        return retval;
    }

    public String getCurrentPermuationString(int count) {
        if (count > m_positions.size()) {
            count = m_positions.size();
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(m_positions.get(i)).append("_");
        }
        if (count > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    private boolean m_beforeFirst = true;
    private List<T> m_elements = new ArrayList<>();
    private List<Integer> m_positions = new ArrayList<>();
}
