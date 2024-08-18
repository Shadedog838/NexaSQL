package Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

/**
 * The Tokenizer class provides methods to tokenize a given SQL-like command string into a list of tokens.
 */
public class Tokenizer {

    /**
     * Tokenizes the given command string into a list of tokens.
     *
     * @param command the command string to be tokenized
     * @return a list of tokens
     * @throws Exception if an error occurs during tokenization
     */
    public static ArrayList<Token> Tokenize(String command) throws Exception {
        ArrayList<Token> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideString = false;

        for (int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);

            if (c == '"' && !insideString) {
                if (currentToken.length() > 0) {
                    tokens.add(createToken(currentToken.toString()));
                    currentToken = new StringBuilder();
                }
                insideString = true;
                currentToken.append(c);
            } else if (c == '"' && insideString) {
                insideString = false;
                currentToken.append(c);
                tokens.add(createToken(currentToken.toString()));
                currentToken = new StringBuilder();
            } else if (insideString) {
                currentToken.append(c);
            } else if (Character.isWhitespace(c)) {
                if (currentToken.length() > 0) {
                    tokens.add(createToken(currentToken.toString()));
                    currentToken = new StringBuilder();
                }
            } else if (isPunctuation(c)) {
                if (currentToken.length() > 0) {
                    tokens.add(createToken(currentToken.toString()));
                    currentToken = new StringBuilder();
                }
                tokens.add(createToken(String.valueOf(c)));
            } else {
                currentToken.append(c);
            }
        }

        if (currentToken.length() > 0) {
            tokens.add(createToken(currentToken.toString()));
        }

        return tokens;
    }

    /**
     * Creates a token from the given string value.
     *
     * @param value the string value to be converted to a token
     * @return the created token
     * @throws Exception if an error occurs during token creation
     */
    public static Token createToken(String value) throws Exception {
        Type type = null;
        if (isKeyword(value.toLowerCase())) {
            type = Type.KEYWORD;
            value = value.toLowerCase();
        } else if (isConstraint(value.toLowerCase())) {
            type = Type.CONSTRAINT;
            value = value.toLowerCase();
        } else if (value.equals(";")) {
            type = Type.SEMICOLON;
        } else if (isNumber(value)) {
            if (value.contains(".")) {
                type = Type.DOUBLE; // Numeric value with decimal point
            } else {
                type = Type.INTEGER; // Numeric value without decimal point
            }
        } else if (value.startsWith("\"") && value.endsWith("\"")) {
            type = Type.STRING;
            value = value.replace("\"", "");
        } else if (isBoolean(value.toLowerCase())) {
            type = Type.BOOLEAN;
            value = value.toLowerCase();
        } else if (isNull(value.toLowerCase())) {
            type = Type.NULL;
            value = value.toLowerCase();
        } else if (value.equals("(")) {
            type = Type.L_PAREN;
        } else if (value.equals(")")) {
            type = Type.R_PAREN;
        } else if (value.equals(",")) {
            type = Type.COMMA;
        } else if (isDataType(value.toLowerCase())) {
            type = Type.DATATYPE;
            value = value.toLowerCase();
        } else if (value.equals("*")) {
            type = Type.ASTERISK;
        } else if (isRelationalOperator(value)) {
            type = Type.REL_OP;
        } else if (isQualifier(value.toLowerCase())) {
            type = Type.QUALIFIER;
            value = value.toLowerCase();
        } else if (isName(value.toLowerCase())) {
            type = Type.NAME;
            value = value.toLowerCase();
        } else {
            MessagePrinter.printMessage(MessageType.ERROR, "Invalid value: " + value);
        }
        return new Token(type, value);
    }

    /**
     * Checks if the given value is a keyword.
     *
     * @param value the value to check
     * @return true if the value is a keyword, false otherwise
     */
    public static boolean isKeyword(String value) {
        List<String> keywords = Arrays.asList(
                "create", "table", "drop", "alter", "and", "or", "update", "set", "delete", "drop", "add",
                "default", "insert", "into", "values", "display", "schema", "display", "info", "select", "from", "where",
                "orderby");
        return keywords.contains(value);
    }

    /**
     * Checks if the given value is a constraint.
     *
     * @param value the value to check
     * @return true if the value is a constraint, false otherwise
     */
    public static boolean isConstraint(String value) {
        List<String> constraints = Arrays.asList("notnull", "primarykey", "unique");
        return constraints.contains(value);
    }

    /**
     * Checks if the given value is a boolean.
     *
     * @param value the value to check
     * @return true if the value is a boolean, false otherwise
     */
    public static boolean isBoolean(String value) {
        return value.equals("true") || value.equals("false");
    }

    /**
     * Checks if the given value is a data type.
     *
     * @param value the value to check
     * @return true if the value is a data type, false otherwise
     */
    public static boolean isDataType(String value) {
        List<String> dataTypes = Arrays.asList("integer", "double", "boolean", "char", "varchar");
        return dataTypes.contains(value);
    }

    /**
     * Checks if the given value is a number.
     *
     * @param value the value to check
     * @return true if the value is a number, false otherwise
     */
    public static boolean isNumber(String value) {
        boolean hasDecimal = false;
        // Allow '-' sign at the beginning for negative numbers
        if (value.startsWith("-")) {
            // Skip the '-' sign and check the rest of the characters
            value = value.substring(1);
        }
        for (char c : value.toCharArray()) {
            if (!Character.isDigit(c)) {
                if (c == '.' && !hasDecimal) {
                    hasDecimal = true;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks if the given value is a relational operator.
     *
     * @param value the value to check
     * @return true if the value is a relational operator, false otherwise
     */
    public static boolean isRelationalOperator(String value) {
        List<String> operators = Arrays.asList("=", ">", "<", ">=", "<=", "!=");
        return operators.contains(value);
    }

    /**
     * Checks if the given value is null.
     *
     * @param value the value to check
     * @return true if the value is null, false otherwise
     */
    public static boolean isNull(String value) {
        return value.equals("null");
    }

    /**
     * Checks if the given value is a valid name.
     *
     * @param value the value to check
     * @return true if the value is a valid name, false otherwise
     */
    public static boolean isName(String value) {
        if (isKeyword(value)) {
            return false;
        }
        // Check if the string is not empty and starts with an alphabetic character
        if (!value.isEmpty() && Character.isAlphabetic(value.charAt(0))) {
            // Check if all characters are alphanumeric
            for (int i = 1; i < value.length(); i++) {
                char c = value.charAt(i);
                if (!Character.isLetterOrDigit(c)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Checks if the given character is a punctuation mark.
     *
     * @param c the character to check
     * @return true if the character is a punctuation mark, false otherwise
     */
    public static boolean isPunctuation(char c) {
        return c == '(' || c == ')' || c == ',' || c == ';' || c == '*';
    }

    /**
     * Checks if the given value is a qualifier.
     *
     * @param value the value to check
     * @return true if the value is a qualifier, false otherwise
     */
    public static boolean isQualifier(String value) {
        return value.contains(".") && value.indexOf(".") == value.lastIndexOf(".")
                && isName(value.substring(0, value.indexOf("."))) && isName(value.substring(value.indexOf(".") + 1));
    }
}
