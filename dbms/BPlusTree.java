package dbms;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a B+ Tree
 *
 * @author Suresh Karki
 * @author Pratistha Shrestha
 * @author Rabeya Bibi
 * @author boadu
 * @version 1.1
 * @since 2019-01-30
 */
public class BPlusTree {

    /**
     * The root node of B+ tree
     */
    private Node rootNode;

    /**
     * Constructor of B+ Tree.
     *
     * @param n maximum number of pointers in a node.
     */
    public BPlusTree(int n) {
    }

    /**
     * Performs find operation for a particular key.
     *
     * @param k the particular key to search for
     * @return FindResult object
     * @see FindResult
     */
    public FindResult find(Key k) {
        return new FindResult();
    }

    /**
     * Retrieves all the records that match the key. Calls the find method to get the first index of LeafNode where the
     * key is present and iterates until we don't get the matching keys
     *
     * @param k the particular key to search for
     * @return all the tuples that match with the key k
     */
    public List<Tuple> printAll(Key k) {
        return new ArrayList<>();
    }

    /**
     * Retrieves all the records whose keys are within the range provided.
     *
     * @param lower the lower bound of the range of key to search for. Includes tha key. if null returns from start to
     *              upper bound
     * @param upper the upper bound of the range of key to search for. Includes the key. if null returns from lower
     *              bound to the end
     * @return all the tuples whose keys are within the range
     */
    public List<Tuple> printRange(Key lower, Key upper) {
        return new ArrayList<>();
    }

    /**
     * Calls FindNode;split;InsertInParent and InsertInLeaf
     *
     * @param k key to be inserted
     * @param p pointer to be inserted in the node (a node pointer or a record pointer)
     * @return whether a split operation is needed
     */
    public boolean insert(Key k, Pointer p) {
        return false;
    }


    /**
     * This function returns the node which should hold our new value
     *
     * @param k Key  the key of the new node to be inserted
     * @return the node which we will insert our new record; nul if B+ tree is empty
     */
    private Node L

    FindNode(Key k)

    /**
     * During insertion, this function splits a node, L into two when the node we have to insert into is full
     * This is done by creating a new node and moving some of the values of the full node into it
     * Create two nodes of same type; One serves as a temporary, T node and the other is the new node, L';
     * size of temporary node should be one greater than the node being split
     * Copy elements of the L into T, and insert the new item into it also
     * Set the next pointer of L to L'
     * Erase everything inside of L
     * Now copy the first half of the items in T into L and copy the other half into L'
     * Set the L next to the first value of L', K'
     * Call Insert in InsertIn Parent method and pass the  parameters L,K',L'
     *
     * @param node Node the node to be split
     */
    private Split(Node node) {
    }

    /**
     * @param L  the node which was split
     * @param K' Key key of the new node to be inserted
     * @param L' Node the new node to be inserted
     *           <p>
     *           if L is a root node, root node, set the key, left child and right child to K',Land L' respectively
     *           if L is not the root node, get the parent of L, parent with getParent
     *           if parent is not full, insert K' and L'
     *           if parent is full split parent
     */
    private InsertInParent(Node L, Key Knew, Node Lnew) {
    }

    /**
     * @param L new node to be inserted
     * @param K Key key of the new node
     * @param P File pointer
     *          <p>
     *          if the B+ tree is empty, create a new leaf node
     *          if B+ tree is not empty:
     *          if the leaf node is not full, insert the new value
     *          else if the leaf node is full, split it
     */
    private InsertInLeaf(LeafNode L, Key K, Pointer) {
    }

    /**
     * @param N  the node whose parent we want
     * @returns the parent node
     */
    private Node parent

    getParent(Node node) {
    }


}
