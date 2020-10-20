package dbms;

import java.util.HashMap;
import java.util.List;

/**
 * Manages the dbms.DataDictionary file and all the actions that can be done to read
 * and write to that catalog.
 *
 * @author Tim Arterbury
 * @author Askar Kuvanychbekov
 * @author Jan Svacina
 * @author Abdul
 * @version 1.0
 * @since 2019-01-17
 */
public class DataDictionary {

    /***
     * Contains B+Tree and some additional information
     * about index of certain table.
     */
    public class Index {
        String BPlusTreeFileName;
        boolean is_primaryIndex;
        BPlusTree bt;

        /**
         * Construcote:
         *
         * @param BPlusTree_fileName name of file where BPlusTree is stored
         * @param isPrimary          boolean to indicate whether the Index is primary
         * @param tree               BPlusTree data structure. If null, it's serialized using BPlusTree_fileName
         */
        public Index(String BPlusTree_fileName, boolean isPrimary, BPlusTree tree) {

        }
    }


    /**
     * Defines a Column which is associated with a Table.
     */
    private class Column {
        String name;
        String dataType;
        int dataLength;

        /**
         * index associated with the Column
         * if it's null, then this Column has no index
         */
        Index index;

        // Constraints
        boolean autoIncrement;
        boolean notNull;
        boolean primaryKey;
        ForeignKey foreignKey;

        /**
         * associates Index with Column
         *
         * @param ind: index to be added
         */
        public void addIndex(Index ind) {

        }
    }

    /**
     * Defines a Table which is associated with a Database.
     */
    private class Table {
        /**
         * Creates a Table definition
         */
        public Table(String name, String filename,
                     String characterEndoding, String parentDatabase,
                     String[] columnNames,
                     String[] dataTypes, int[] dataLengths) {

        }

        /**
         * Creates temporary tables with given list of tuples
         * and list of columns.
         * Table created by this constructor is not in a database.
         * Rather, it is used when for temporary tables created by queries,
         * such as select and project.
         */
        public Table(List<Tuple> tuples, List<Column> columns) {

        }

        private String name;
        private String filename;
        private String characterEncoding;

        /**
         * Column name to the Column itself.
         */
        private HashMap<String, Column> columns;

        private List<Integer> pageIds;

        public List<Integer> getPageIds() {
            return null;
        }

        public void setPageIds(List<Integer> pageIds) {

        }

        /**
         * Creates the columns and puts them in columns HashMap.
         * Called in constructor.
         */
        private void createColumns(String[] columnNames,
                                   String[] dataTypes, int[] dataLengths) {

        }
    }

    /**
     * Defines a Database.
     */
    private class Database {
        public Database(String name,
                        String defaultCharacterEncoding, List<Table> tables) {

        }

        public void addTable(Table table) {

        }

        public void removeTable(Table table) {

        }

        private String name;
        private String defaultCharacterEncoding;

        /**
         * Table name to the Table itself.
         */
        private HashMap<String, Table> tables;
    }

    /**
     * Creates a dbms.DataDictionary with default catalog file.
     * If the default catalog file name is found, it will be loaded. If not,
     * the function will create a new catalog file with a default name.
     */
    public DataDictionary() {

    }

    /**
     * Uses the FileManager to create a file for the database with the
     * specified database name. And files for each of the associated.
     * Tables.
     *
     * @param database database instance
     * @see FileManager
     */
    public void createDatabase(Database database) {

    }

    /**
     * Creates a Table file using the FileManager, associates it with a
     * database, and stores its column names, data types, and data lengths
     * in the system catalog file.
     *
     * @param table        table instance
     * @param databaseName name of the database
     * @see FileManager
     */
    public void createTableForDatabase(Table table, String databaseName) {

    }


    /**
     * Gives the PageIds of a specific Table.
     *
     * @param tableName    name of the table to create
     * @param databaseName name of the database
     * @return PageIds of the table
     */
    public List<Integer> getTable(String databaseName, String tableName) {
        return null;
    }

    /**
     * After every modification to this dbms.DataDictionary class, this method is
     * called to flush the class data into the file.
     */
    private void writeClassToFile() {

    }

    /**
     * Filename where system Catalog information is stored.
     */
    private final String dataDictionaryFilename = "datadictionary";

    /**
     * Holds database definition information as well as all Table definition
     * information. This includes table file names, table columns, data types,
     * etc, but does not contain all tuples (rows) from these constructs.
     * These rows are contained in files.
     */
    HashMap<String, Database> databases;


}
