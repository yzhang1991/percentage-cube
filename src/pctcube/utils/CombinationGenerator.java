package pctcube.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CombinationGenerator<T> implements Iterable<ArrayList<T>>, Iterator<ArrayList<T>> {

    public CombinationGenerator() { }

    public CombinationGenerator(List<T> elements) {
        addElements(elements);
    }

    public void addElement(T element) {
        m_elements.add(element);
        m_selected.add(1);
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

    public void setNumOfElementsToSelect(int value) {
        m_numOfElementsToSelect = value;
        reset();
    }

    public int getNumOfElementsToSelect() {
        return m_numOfElementsToSelect;
    }

    private void select(int numOfElements) {
        reset();
        if (numOfElements > m_elements.size()) {
            numOfElements = m_elements.size();
        }
        for (int i = 0; i < numOfElements; i++) {
            m_selected.set(i, 1);
        }
        for (int i = numOfElements; i < m_elements.size(); i++) {
            m_selected.set(i, 0);
        }
    }

    @Override
    public boolean hasNext() {
        if (m_beforeFirst) {
            return true;
        }
        if (m_elements.size() <= 1) {
            return false;
        }
        int i = m_elements.size() - 2;
        while (i >= 0 && m_selected.get(i) <= m_selected.get(i+1)) {
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
            select(m_numOfElementsToSelect);
            m_beforeFirst = false;
            return getCurrentCombination();
        }
        if (m_elements.size() <= 1) {
            throw new NoSuchElementException();
        }
        int i = m_elements.size() - 2;
        while (i >= 0 && m_selected.get(i) <= m_selected.get(i+1)) {
            i--;
        }
        if (i < 0) {
            throw new NoSuchElementException();
        }
        m_selected.set(i, 0);
        m_selected.set(i+1, 1);
        int remainingSelectedElementsCount = 0;
        for (int j = i + 2; j < m_elements.size(); j++) {
            remainingSelectedElementsCount += m_selected.get(j);
        }
        for (int j = i + 2; j < m_elements.size(); j++) {
            if (remainingSelectedElementsCount > 0) {
                m_selected.set(j, 1);
            }
            else {
                m_selected.set(j, 0);
            }
            remainingSelectedElementsCount--;
        }
        return getCurrentCombination();
    }

    @Override
    public Iterator<ArrayList<T>> iterator() {
        return this;
    }

    private ArrayList<T> getCurrentCombination() {
        ArrayList<T> retval = new ArrayList<>();
        for (int i = 0; i < m_elements.size(); i++) {
            if (m_selected.get(i) > 0) {
                retval.add(m_elements.get(i));
            }
        }
        return retval;
    }

    public List<Integer> getCurrentSelectionFlags() {
        return Collections.unmodifiableList(m_selected);
    }

    public List<Integer> getCurrentSelectionIndices() {
        List<Integer> retval = new ArrayList<>();
        for (int i = 0; i < m_elements.size(); i++) {
            if (m_selected.get(i) > 0) {
                retval.add(i);
            }
        }
        return retval;
    }

    private boolean m_beforeFirst = true;
    private int m_numOfElementsToSelect = 0;
    private List<T> m_elements = new ArrayList<>();
    private List<Integer> m_selected = new ArrayList<>();
}
