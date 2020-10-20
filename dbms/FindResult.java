package dbms;


/**
 * Class that represents a result after find operation on a key in a B+ tree
 *
 * @author Suresh Karki
 * @author Pratistha Shrestha
 * @author Rabeya Bibi
 * @version 1.0
 * @since 2019-01-30
 */
public class FindResult {

    /**
     * Node where the key is/should be stored
     */
    private Node node;

    /**
     * Represents exact index of key if the find operation was successful
     * This will only be used during the search operation, not the insertion operation.
     */
    private int index;

    /**
     * Represents whether the find operation was successful in finding the exact key
     */
    private boolean found;
}
