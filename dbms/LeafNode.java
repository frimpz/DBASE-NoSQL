package dbms;

import java.util.List;

/**
 * Represents a Leaf Node in a B+ Tree
 *
 * @author Suresh Karki
 * @author Pratistha Shrestha
 * @author Rabeya Bibi
 * @author boadu
 * @version 1.1
 * @since 2019-01-30
 * @since
 */
public class LeafNode extends Node {

    /**
     * pointers inside a node that point to file records.
     */
    private List<Object> recordPointers;

    /**
     * pointer to the next Leaf Node in the same level. It will be null if this is the last Leaf Node.
     */
    private LeafNode nextLeafNode;

    /**
     * Constructor of leaf node. Initializes the list of keys and pointers
     *
     * @param n maximum number of pointers in a node.
     */
    public LeafNode(int n) {
    }

    /**
     * Performs insertion operation in a leaf node of the B+ tree.
     * As all the keys has to be in sorted order, it finds the index in which to place the k and p
     * and then shuffles the remaining keys and pointers if needed.
     *
     * @param n leaf node in which key has to be inserted
     * @param k key to be inserted
     * @param P record-pointer i.e. the pointer to a tuple with the key k.
     */
    public void insert(Key k, Pointer p) {
    }

    /**
     * Sets the next leaf node pointer
     *
     * @param nextLeafNode LeafNode
     */
    public void setNextPointer(LeafNode nextLeafNode) {
    }

}
