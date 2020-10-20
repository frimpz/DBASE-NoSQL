package dbms;

import java.util.Arrays;

/**
 * The Page class represents a block of data returned from the file system. It
 * is the smallest amount of data that can be read or written to the underlying
 * file system. The Page class contains three specific types of data, the
 * page metadata, the rows of data stored in a table and the footers used for
 * locating rows of data. The metadata will be expanded as needed.
 *
 * @author Speeegle
 */

public class Page {

    String pageId;
    String fileName;
    String tableName;
    int freeSpace;
    int unusedSpace;
    int freePointer;
    int unusedPointer;
    int footerSize;
    boolean dirtyBit;
    boolean pinnedBit;
    int[] footer = new int[footerSize]; // Note that this does not work
    Tuple[] data = new Tuple[footerSize]; // Note that this does not work

    /**
     * The Page constructor represents fetching a page from the underlying
     * file system. Effectively, the constructor is called whenever
     * the FileManage executes a readFile call on a block that is not
     * currently in main memory. The metadata fields are populated by
     * `	* the values stored with the page. The pageId uniquely defines the
     * physical location of the page.
     * <p>
     * An array of tuples is createad for memory access to all of the rows stored on the page.
     *
     * @param rawData The representation of the raw data retireved from the disk
     * @throws Exception If the data is corrupted
     **/

    public Page(byte[] rawData) throws Exception {
        for (int i = 0; i < footerSize; i++) {
            data[i] = new Tuple(tableName, Arrays.copyOfRange(rawData, footer[i], footer[i + 1])); // Note that this does not work
        }
    }

    /**
     * The fetch method returns the Tuple object from the internal array.
     * The offset is the footer number (not its value) of the
     * row.
     *
     * @param offset The footer number of the row
     * @return The tuple object representing the data
     * @throws Exception Invalid offset or tuple deleted
     **/

    public Tuple parse(int offset) throws Exception {
        return data[offset];
    }

    /**
     * The insert method adds a Tuple to the page. A
     * new footer is created and all of the metadata is updated.
     * The method returns true if successful and false if
     * unsuccessful.
     *
     * @param t The tuple to be added
     * @throws Exception Invalid tuple
     * @return boolean indicating success
     **/

    public boolean insert(Tuple t) throws Exception {
        data[footerSize] = t;
        footerSize++;
        return true;
    }

    /**
     * The delete method removes a Tuple at offset from the page.
     * The tuple is deleted by setting the delete bit
     * to true; adding the space consumed by the tuple
     * to the unused space; and adding the start of the tuple
     * to the unused space chain.
     *
     * @param offset The tuple to be removed
     * @throws Exception Invalid tupleId
     * @return boolean indicating success
     **/

    public boolean delete(int offset) throws Exception {
        data[offset].deleted = true;
        return true;

    }

    /**
     * The update method replaces a Tuple at location tupleId from the page
     * with the provided tuple. Assuming fixed size allocations, the new
     * tuple shold consume exactly the same space as the old tuple, and
     * thereform occupy the same location on the page.
     *
     * @param t       The new tuple to be inserted
     * @param tupleId The tuple to be removed
     * @throws Exception invalid tupleid, tuple mismatch
     * @return boolean indicating success
     **/

    public boolean update(Tuple t, int tupleId) throws Exception {
        return true;

    }
}

