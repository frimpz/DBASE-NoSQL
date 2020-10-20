package dbms;

/**
 * The Tuple class represents a single row of data from a table.
 * It contains metadata such as the table nameof the row and a delete bit.
 **/

public class Tuple {

    String tableName;
    boolean deleted;
    Object[] values;

    /**
     * The Tuple constructor is called whenever a new row is needed.
     * The size of the values array is determined by the metadata for
     * the table.
     *
     * @param tableName Name of the table for the row
     * @throws Exception If data does not match table
     */

    public Tuple(String tableName) throws Exception {

    }

    /**
     * The Tuple constructor is called whenever an existing row is
     * read.  The size of the values array is determined by the metadata for
     * the table. The data is parsed according to the metadata.
     *
     * @param tableName Name of the table for the row
     * @param data      Data from existing row; omitted if new row
     * @throws Exception If data does not match table
     */

    public Tuple(String tableName, byte[] data) throws Exception {

    }

    /**
     * The getInt method returns the value of an attribute based on
     * the name of the attribute.
     *
     * @param attrName Name of the attribute
     * @return value        Value of the attribute
     * @throws Exception Invalid attribute name or type mismatch
     */

    public int getInt(String attrName) throws Exception {
        int loc = 0; // determined by metadata lookup
        int value = (int) values[loc]; // Value in tuple
        return value;
    }

    /**
     * The getDouble method returns the value of an attribute based on
     * the name of the attribute.
     *
     * @param attrName Name of the attribute
     * @return value        Value of the attribute
     * @throws Exception Invalid attribute name or type mismatch
     */

    public double getDouble(String attrName) throws Exception {
        int loc = 0; // determined by metadata lookup
        double value = (double) values[loc]; // Value in tuple
        return value;
    }


    /**
     * The getString method returns the value of an attribute based on
     * the name of the attribute.
     *
     * @param attrName Name of the attribute
     * @return value        Value of the attribute
     * @throws Exception Invalid attribute name or type mismatch
     */

    public String getString(String attrName) throws Exception {
        int loc = 0; // determined by metadata lookup
        String value = (String) values[loc]; // Value in tuple
        return value;
    }

    /**
     * The getObject method returns the value of an attribute based on
     * the name of the attribute.
     *
     * @param attrName Name of the attribute
     * @return value        Value of the attribute
     * @throws Exception Invalid attribute name or type mismatch
     */

    public Object getObject(String attrName) throws Exception {
        int loc = 0; // determined by metadata lookup
        Object value = values[loc]; // Value in tuple
        return value;
    }

    /**
     * The setAttr method sets the value of an attribute based on
     * the name of the attribute.
     *
     * @param attrName Name of attribute to update
     * @param value    New value of attribute
     * @throws Exception Attribute does not exist or type mismatch
     */

    public void setAttr(String attrName, Object value) throws Exception {

    }

}
