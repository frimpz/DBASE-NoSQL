import java.util.HashMap;
import java.util.Map;

/**
 * The BufferManager class maintains the entire operation for maintaining the copies of disk blocks in the main memory.
 * It is a singleton class and it is injected in the classes which requires it.
 *
 * @author Sanjel
 * @author Shrestha
 */
public class BufferManager {

    /**
     * The BufferManager constructor is executed when the database begins. Initially it allocaltes
     * memory for storing the copies of certain blocks of the disk drive. The framePageMap is initialized when the
     * constructor runs.
     * If the BufferManager constructor fails, it throws a fatal exception and the
     * database must restart the service.
     */
    public BufferManager() throws Exception {

    }


    /**
     * The framePage map maintains the mapping between the frames of the main memory and identifier of pages it stores.
     * It provides information about which page is mapped to which frame in the main memory.
     */
    private Map<Integer, Integer> framePageMap = new HashMap<>();

    /**
     * The handleRequestFromProgam method handles the request sent by the caller program within the dbms.
     * It is a wrapper function which incorporates all the functionalities of the buffer manager by calling other worker functions.
     * Calls the checkPage() to see if the desired block is in the memory.
     * If the checkPage() returns true then the memory address is returned.
     * Else If the checkPage() returns false then the readPageFromDisk method is called to fetch the page from the disk.
     * If the memory is full then removeBlockFromMemory method is called to free up space in the main memory.
     *
     * @param tablename name of table passed by the caller.
     * @param pageId    pageId of the corresponding page.
     * @return Memory address in the main memory at which the block resides
     */
    public Long handleRequestFromProgram(String tablename, int pageId) {

        return null;
    }

    /**
     * The checkPage method checks whether the block with the certain Page Id is in the main memory or not.
     *
     * @param pageId Page Id of the
     * @return True if the page if found in the main memory, else it returns false.
     */
    private boolean checkPage(int pageId) {

        return true;
    }

    /**
     * The readPageFromDisk method reads the block from the disk with the given pageId.
     * This method is called by the handleRequestFromProgram method when the desired page is not found in the main memory.
     *
     * @param pageId Page Id of the block which is to be accessed
     */
    private void readPageFromDisk(int pageId) {

    }

    /**
     * The removeBlockFromMemory method deletes a block from the main memory.
     * This method is called when the main memory is full and the buffer manager needs to write a new block from disk to main memory.
     * Different strategies like LRU, MRU, toss immediate can be used to decide which block to remove from the memory.
     * Right now statistical information from the data dictionary is assumed to be available. This information can be used to decide which block is to be removed from the memory.
     * writePageToDisk method is called if the block which is removed has been modified and needs to be updated to disk.
     */
    private void removeBlockFromMemory() {

    }

    /**
     * The writePageToDisk method writes the block from the main memory to the disk.
     * This method is called when the buffer manager needs to replace a block and update that replaced block to the disk if it is modified.
     * This method uses the FileManager's writeFile method in order to write the page to the disk.
     *
     * @param page The page which is to be written to the disk
     */
    private void writePageToDisk(Page page) {

    }

}
