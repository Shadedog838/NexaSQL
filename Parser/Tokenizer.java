package Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

public class Tokenizer {

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

	public static boolean isKeyword(String value) {
		List<String> keywords = Arrays.asList(
				"create", "table", "drop", "alter", "and", "or", "update", "set", "delete", "drop", "add",
				"default", "insert", "into", "values", "display", "schema", "display", "info", "select", "from", "where",
				"orderby");
		return keywords.contains(value);
	}

	public static boolean isConstraint(String value) {
		List<String> constraints = Arrays.asList("notnull", "primarykey", "unique");
		return constraints.contains(value);
	}

	public static boolean isBoolean(String value) {
		return value.equals("true") || value.equals("false");
	}

	public static boolean isDataType(String value) {
		List<String> dataTypes = Arrays.asList("integer", "double", "boolean", "char", "varchar");
		return dataTypes.contains(value);
	}

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

	public static boolean isRelationalOperator(String value) {
		List<String> operators = Arrays.asList("=", ">", "<", ">=", "<=", "!=");
		return operators.contains(value);
	}

	public static boolean isNull(String value) {
		return value.equals("null");
	}

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

	public static boolean isPunctuation(char c) {
		return c == '(' || c == ')' || c == ',' || c == ';' || c == '*';
	}

	public static boolean isQualifier(String value) {
		return value.contains(".") && value.indexOf(".") == value.lastIndexOf(".")
				&& isName(value.substring(0, value.indexOf("."))) && isName(value.substring(value.indexOf(".") + 1));
	}
}