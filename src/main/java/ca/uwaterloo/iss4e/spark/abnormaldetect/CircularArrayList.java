package ca.uwaterloo.iss4e.spark.abnormaldetect;

import java.util.*;

/**
 * Created by xiuli on 6/29/15.
 */
public class CircularArrayList<E>
        extends AbstractList<E> implements RandomAccess,Cloneable, java.io.Serializable {

    private final int n; // buffer length
    private final List<E> buf; // a List implementing RandomAccess
    private int head = 0;
    private int tail = 0;

    public CircularArrayList(int capacity) {
        n = capacity + 1;
        buf = new ArrayList<E>(Collections.nCopies(n, (E) null));
    }

    public int capacity() {
        return n - 1;
    }

    public boolean isFull(){
        return size()==capacity();
    }

    private int wrapIndex(int i) {
        int m = i % n;
        if (m < 0) { // java modulus can be negative
            m += n;
        }
        return m;
    }

    // This method is O(n) but will never be called if the
    // CircularArrayList is used in its typical/intended role.
    private void shiftBlock(int startIndex, int endIndex) {
        assert (endIndex > startIndex);
        for (int i = endIndex - 1; i >= startIndex; i--) {
            set(i + 1, get(i));
        }
    }

    @Override
    public int size() {
        return tail - head + (tail < head ? n : 0);
    }

    @Override
    public E get(int i) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
        return buf.get(wrapIndex(head + i));
    }

    @Override
    public E set(int i, E e) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
        return buf.set(wrapIndex(head + i), e);
    }

    @Override
    public void add(int i, E e) {
        int s = size();
        if (s == n - 1) {
            throw new IllegalStateException("Cannot add element."
                    + " CircularArrayList is filled to capacity.");
        }
        if (i < 0 || i > s) {
            throw new IndexOutOfBoundsException();
        }
        tail = wrapIndex(tail + 1);
        if (i < s) {
            shiftBlock(i, s);
        }
        set(i, e);
    }

    @Override
    public E remove(int i) {
        int s = size();
        if (i < 0 || i >= s) {
            throw new IndexOutOfBoundsException();
        }
        E e = get(i);
        if (i > 0) {
            shiftBlock(0, i);
        }
        head = wrapIndex(head + 1);
        return e;
    }
    public static void main(String[]args){
        CircularArrayList clist = new CircularArrayList<Integer>(3);
        clist.add(1);
        clist.add(2);
        clist.add(3);
        clist.add(4);

        for (int i=0; i<3; ++i){
            System.out.println(clist.get(i));
        }
    }
}