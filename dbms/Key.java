package dbms;


/**
 * Class that represents an index key in B+ Tree
 *
 * @author Suresh Karki
 * @author Pratistha Shrestha
 * @author Rabeya Bibi
 * @version 1.0
 * @since 2019-01-30
 */
public class Key implements Comparable {

    /**
     * value of the key
     */
    private Object value;

    /**
     * Compares objects and return whether the given object is greater, less or equal.
     *
     * @param k object to be compared against
     * @return whether the object is greater, less or equal to this object
     */
    @Override
    public int compareTo(Object k) {
        return 1;
    }
}
