package dbms;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a relational algebra expression.
 * <p>
 * Condition is used to define the single condition for select statement.
 * Ex: "Table1.age = 18" is a condition.
 * Predicate class is used to define multiple conditions connected by
 * logical operators.
 * Ex: " (Table1.age = 18) and (Table1.GPA =3.3)" is a predicate
 * composed of two conditions.
 *
 * @author Tim Arterbury
 * @author Askar Kuvanychbekov
 * @author Jan Svacina
 * @author Abdul
 * @version 1.0
 * @since 2019-01-17
 */

public class Predicate {
    /**
     * Constructs a dbms predicate.Predicate based on a statement of Operators, Attributes,
     * and values.
     *
     * @param statement list of Operators, Attributes, and values, making a
     *                  making a relational algebra expression.
     */
    Predicate(List<Object> statement) {

    }

    /**
     * Used to define the single condition for select statement.
     * Ex: "Table1.age = 18" is a condition.
     * Predicate class is used to define multiple conditions connected by
     * logical operators.
     * Ex: " (Table1.age = 18) and (Table1.GPA =3.3)" is a predicate
     * composed of two conditions.
     */
    public class Condition {
        /**
         * An attribute from a table.
         */
        public class Attribute {
            String attributeName;
            String tableName;
        }

        /**
         * Constructor
         *
         * @param conditionStatement list of MathOperators, Attributes, and values, making a
         *                           making a relational algebra expression.
         */
        public Condition(List<Object> conditionStatement) {

        }

        /**
         * An ordered array of Attributes, values and mathematical operators.
         * Follows order of operations this order may be
         * defined somewhere in the implementation.
         */
        public ArrayList<Object> conditionStatement;
    }

    /**
     * Operations that may be done on Attributes and values in a dbms.
     */
    public enum MathOperators {
        EQUAL, GREATER, LESS, GREATER_OR_EQUAL, LESS_OR_EQUAL
    }

    ;

    /**
     * Operations that may be done on Conditions in a dbms
     */
    public enum LogicalOperators {
        AND, OR, NOT, LEFT_PAREN, RIGHT_PAREN
    }

    ;


    /**
     * An ordered array of Conditions, and logical operators, on those
     * conditions. Follows order of operations this order may be
     * defined somewhere in the implementation.
     */
    public ArrayList<Object> statement;
}
