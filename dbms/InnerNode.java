package dbms;

import java.util.List;

/**
 * Represents an inner node in a B+ Tree
 *
 * @author Suresh Karki
 * @author Pratistha Shrestha
 * @author Rabeya Bibi
 * @version 1.0
 * @since 2019-01-30
 */
public class InnerNode extends Node {

    /**
     * pointers inside a non-leaf node pointing to child Nodes
     */
    private List<Node> childPointers;

    /**
     * Constructor of inner node. Initializes the list of keys and pointers
     *
     * @param n maximum number of pointers in a node.
     */
    public InnerNode(int n) {
    }

    /**
     * Performs insertion operation in a non-leaf node of the B+ tree.
     * It is a recursive function.
     * Its base case is such that, when n is the root of the tree, it creates a new node (say R) with pointers n and P and key k.
     * Then makes R a root node of the B+ tree.
     * Otherwise, it checks whether n.parentNode (say Par) has enough space to hold another key k and pointer P.
     * If yes, it inserts K and P in Par node maintaining its sorted order.
     * Or else, it follows the same operation as it was done for splitting a leaf-node and, divides P  into P and P',
     * and go so-on and so-forth with a key k (which is always the least value of P'), until the base-case gets satisfied.
     *
     * @param n non-leaf node in which key has to be inserted
     * @param k key to be inserted
     * @param P pointer to a child node of node n that has all the keys that are greater or equal to the key k
     */
    public void insertInInnerNode(Node n, Key k, Object P) {
    }

}
