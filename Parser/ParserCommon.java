package Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

public class ParserCommon {

    public static ArrayList<Token> Tokenize(String ddlStatement) throws Exception {
        ArrayList<Token> tokens = new ArrayList<>();
        String currentToken = "";

        boolean label = false;
        boolean number = false;
        boolean hasDecimal = false;
        boolean string = false;

        for (int i = 0; i < ddlStatement.length(); i++) {
            char c = ddlStatement.charAt(i);

            if (c == '(' && !label && !number && !string) {
                tokens.add(new Token(Type.L_PAREN, "("));
            } else if (c == ')' && !label && !number && !string) {
                tokens.add(new Token(Type.R_PAREN, ")"));
            } else if (c == ';' && !label && !number && !string) {
                tokens.add(new Token(Type.SEMICOLON, ";"));
            } else if (c == ',' && !label && !number && !string) {
                tokens.add(new Token(Type.COMMA, ","));
            } else if (c == '*' && !label && !number && !string) {
                tokens.add(new Token(Type.ASTERISK, "*"));
            } else if (isRelationalOperator(c)) {
                StringBuilder relationalOperator = new StringBuilder();
                relationalOperator.append(c);
                if (c == '!' && ddlStatement.charAt(i + 1) == '=') {
                    relationalOperator.append('=');
                    i++;
                } else if ((c == '>' || c == '<') && ddlStatement.charAt(i + 1) == '=') {
                    relationalOperator.append('=');
                    i++;
                }
                tokens.add(new Token(Type.REL_OP, relationalOperator.toString()));
            } else if (c == ' ' && !label && !number && !string) {
                // Ignore whitespace
            } else if (c == '\"') {
                if (string) {
                    currentToken += c;
                    currentToken = currentToken.replace("\"", "");
                    tokens.add(new Token(Type.STRING, currentToken));
                    currentToken = "";
                    string = false;
                    label = false;
                } else {
                    string = true;
                    currentToken += c;
                }
            } else if ((Character.isDigit(c) || (c == '.' && !hasDecimal) || (c == '-' && currentToken.isEmpty())) && !label) {
                if (c == '.') {
                    hasDecimal = true;
                }
                currentToken += c;
                number = true;
            } else if (Character.isLetterOrDigit(c) || (c == '.' && !hasDecimal)) {
                if (c == '.') {
                    hasDecimal = true;
                }
                currentToken += c;
                label = true;
            } else {
                currentToken = currentToken.toLowerCase();
                if (string) {
                    currentToken += c;
                    continue;
                }
                if (number && !currentToken.endsWith("-")) {
                    tokens.add(createNumberToken(currentToken, hasDecimal));
                } else if (label && hasDecimal) {
                    tokens.add(createTokenWithType(currentToken, Type.QUALIFIER));
                } else if (label) {
                    tokens.add(createLabelToken(currentToken));
                } else {
                    MessagePrinter.printMessage(MessageType.ERROR, "Invalid token " + currentToken);
                }
                currentToken = "";
                number = false;
                label = false;
                hasDecimal = false;
                i--;
            }
        }

        if (!currentToken.isEmpty()) {
            tokens.add(createTokenWithType(currentToken, Type.NAME));
        }

        return tokens;
    }



    private static boolean isRelationalOperator(char c) {
        return c == '=' || c == '>' || c == '<' || c == '!';
    }

    private static Token createNumberToken(String value, boolean hasDecimal) throws Exception {
        if (hasDecimal) {
            return createTokenWithType(value, Type.DOUBLE);
        } else {
            return createTokenWithType(value, Type.INTEGER);
        }
    }

    private static Token createLabelToken(String value) throws Exception {
        value = value.toLowerCase();
        if (isKeyword(value)) {
            return createTokenWithType(value, Type.KEYWORD);
        }
        switch (value) {
            case "notnull":
            case "primarykey":
            case "unique":
                return createTokenWithType(value, Type.CONSTRAINT);
            case "integer":
            case "double":
            case "boolean":
            case "char":
            case "varchar":
                return createTokenWithType(value, Type.DATATYPE);
            case "null":
                return createTokenWithType(value, Type.NULL);
            case "true":
            case "false":
                return createTokenWithType(value, Type.BOOLEAN);
            default:
                return createTokenWithType(value, Type.NAME);
        }
    }

    private static Token createTokenWithType(String value, Type type) throws Exception {
        return new Token(type, value);
    }

    public static boolean isKeyword(String value) {
		List<String> keywords = Arrays.asList(
				"create", "table", "drop", "alter", "and", "or", "update", "set", "delete", "drop", "add",
				"default", "insert", "into", "values", "display", "schema", "display", "info", "select", "from", "where", "orderby");
		return keywords.contains(value);
	}
}