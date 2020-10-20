package dbms;

import java.util.stream.Stream;

/**
 * Manages getting Tuples based on queries.
 *
 * @author Tim Arterbury
 * @author Askar Kuvanychbekov
 * @author Jan Svacina
 * @author Abdul
 * @version 1.0
 * @since 2019-01-17
 */
public class QueryManager {
    /**
     * Selects rows from a Table based on a given predicate.
     * Checks if the predicate has only single Condition or not and
     * acts accordingly.
     *
     * @param tableName name of the table to select from.
     * @param predicate relational algebra expression to select rows with.
     * @return set of rows (Tuples) that match the predicate.
     */
    public Stream<Tuple> select(String tableName, Predicate predicate) {
        return null;
    }


    /**
     * Evaluates whether or not to keep a Tuple based on a predicate.
     * Used by the getResults function in LinearSelection class.
     *
     * @param tuple     a row
     * @param predicate relational algebra expression to select rows with.
     * @return if the tuple satisfies the predicate or not
     */
    private boolean evaluateTuple(Tuple tuple, Predicate predicate) {
        return false;
    }

    /**
     * Given TableName and Condition(condition contains information about which
     * column/attribute is used) checks if the column has index, if the condition
     * is on key, and then computes the estimated costs of the selection operation
     * with different strategies. Returns the Strategy class that have minimum cost.
     * Assigns Index class pointers in Strategy class to the needed Index.
     *
     * @param TableName name of the table to select from.
     * @param condition relational algebra expression to select rows with.
     * @return Strategy class that selects required rows/tuples.
     */
    private Strategy calculateBestCost(String TableName, Predicate.Condition condition) {
        return null;
    }


    /**
     * Represents the strategy used to select required rows.
     * Classes that implement this interface must implement getResults function
     * that selects required rows/tuples according to the Condition.
     * Index is used in some of them.
     */
    public interface Strategy {
        /**
         * Selects required rows/tuples according to the Condition.
         * Index is used in some of them.
         *
         * @param TableName name of the table to select from.
         * @param condition relational algebra expression to select rows with.
         */
        void getResults(String TableName, Predicate.Condition condition);
    }

    public class LinearSelection implements Strategy {
        @Override
        public void getResults(String TableName, Predicate.Condition condition) {

        }

    }

    public class PrimaryIndexSelection implements Strategy {
        private DataDictionary.Index ind;

        @Override
        public void getResults(String TableName, Predicate.Condition condition) {

        }

    }

    public class SecondaryIndexSelection implements Strategy {
        private DataDictionary.Index ind;

        @Override
        public void getResults(String TableName, Predicate.Condition condition) {

        }
    }

}
