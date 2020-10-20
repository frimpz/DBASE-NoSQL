package dbms;

import java.util.List;

/**
 * Represents a Node in a B+ Tree
 *
 * @author Suresh Karki
 * @author Pratistha Shrestha
 * @author Rabeya Bibi
 * @version 1.0
 * @since 2019-01-30
 */
public abstract class Node {

    /**
     * Keys stored inside a node
     */
    private List<Key> keys;

    /**
     * Points to the parent Node. is null if the node is root.
     */
    private Node parentNode;

}
