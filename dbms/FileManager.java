package dbms;

/**
 * FileManager is the singleton class for managing files for accessing
 * data. This represents a service provided by the DBMS.
 * In our system, we make the simplifying assumption that all data is stored
 * in consecutive blocks on the disk and all partitioning occurs below the level
 * of the FileManager. The FileManager supports random access to a block of data,
 * but does not support sequential reads.
 * <p>
 * The FileManager uses the Page class for returning data.
 *
 * @author Speegle
 */
public class FileManager {

    int pgSize = 1; // Value based on operating system

    /**
     * The FileManager constructor is executed when the database begins. At initiation,
     * it allocates needed resources, such as internal memory for the storage of open
     * files. If the FileManager constructor fails, it throws a fatal exception and the
     * database must restart the service. If the service cannot be restarted, the
     * database cannot execute normally.
     *
     * @throws Exception Insufficient resources to execute
     */
    public FileManager() throws Exception {
    }

    /**
     * The newFile method is called when new disk storage is needed. Typically, this occurs
     * when a table is created.  The fileManager requests a default number of pages from the operating system.
     * Each of these pages are formatted and rewritten to disk. The information is added to
     * the appropriate metadata. The new fileId is stored locally.
     * Exception can be thrown if there is insufficient disk space to allocate for the
     * file.
     *
     * @param fileName The name of the file to be created
     * @throws Exception Insufficient disk space
     * @return The fileId from the local data for the created file
     */
    public int newFile(String fileName) throws Exception {
        int fileId = 0;
        return fileId;
    }

    /**
     * The openFile method returns a fileId (int) for accessing data. The fileId is valid
     * only within a specific FileManager instance. The openFile method uses metadata
     * to learn the operating system specific parameters for the file.
     * An exception is thrown if the file cannot be opened, such as if the table is not
     * in the metadata. The FileManager keeps a count of the number of transactions
     * with an open request on each file.
     *
     * @param tableName The name of the table to be opened
     * @return The id of the file being accessed
     * @throws Exception Exception thrown if no file exists
     */
    public int openFile(String tableName) throws Exception {
        int fileId = 0; // Actually, metadata value
        return fileId;
    }

    /**
     * The readFile method returns a Page object of data (without processing) from the
     * disk belonging to the specific file. The method uses the internal
     * data structure to determine the disk location of the first page in
     * the file (or it can rely on the operating system). In either case, the
     * fileId + offset uniquely determines a block of data.
     *
     * @param fileId The ID of the file to be accessed
     * @param offset The page number within the file to be accessed
     * @throws Exception Exception thrown if invalid fileId, invalid pageId or no such file
     * @return A Page (representing a page of data)
     */
    public Page readFile(int fileId, int offset) throws Exception {
        byte[] data = new byte[pgSize]; // read from file
        Page p = new Page(data);
        return p;
    }

    /**
     * The readFile method returns a Page object of data (without processing) from the
     * disk at the specific pageId.
     *
     * @param pageId Representation of unique physical location of the page
     * @throws Exception Exception thrown if invalid pageId
     * @return A Page (representing a page of data)
     */
    public Page readFile(String pageId) throws Exception {
        byte[] data = new byte[pgSize]; // read from file
        Page p = new Page(data);
        return p;
    }

    /**
     * The writeFile method modifies the disk by writing a Page object back to the disk.
     * The page is written to the pageId location specified by the page.
     * <p>
     * Note that all data previously on the block is lost.
     *
     * @param data The page object representing the data to be written
     * @throws Exception Exception thrown for invalid fileId, invalid pageId or no such file
     */

    public void writeFile(Page data) throws Exception {
    }

    /**
     * The extendFile method allocates a new empty block at the end of the disk space
     * for the file. The disk is formatted appropriately.
     * The internal data structure and the metadata are updated to include the
     * new block.
     *
     * @param fileId The ID of the file to be extended
     * @throws Exception Invalid fileId
     */
    public void extendFile(int fileId) throws Exception {
    }

}
