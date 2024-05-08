package Parser;

import StorageManager.Objects.AttributeSchema;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;
import StorageManager.TableSchema;

import java.util.ArrayList;
import java.util.HashMap;

public class DDLParser {

	public static TableSchema parseCreateTable(ArrayList<Token> tokens) throws Exception {
		ArrayList<AttributeSchema> attributes = new ArrayList<AttributeSchema>();
		String tableName = "";

		tokens.remove(0); // remove create token
		tokens.remove(0); // remove table token

		if (tokens.get(0).getType() != Type.NAME) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected table name");
		}

		tableName = tokens.remove(0).getVal();

		if (tokens.get(0).getType() != Type.L_PAREN) {
			MessagePrinter.printMessage(MessageType.ERROR, "Open parenthesis expected in create table stmt!");
		}
		tokens.remove(0);

		while (tokens.get(0).getType() != Type.R_PAREN) {
			String attributeName = "";
			String dataType = "";
			boolean notNull = false;
			boolean primaryKey = false;
			boolean unique = false;

			if (tokens.get(0).getType() != Type.NAME) {
				MessagePrinter.printMessage(MessageType.ERROR, "Expected attribute name");
			}

			attributeName = tokens.remove(0).getVal();

			if (tokens.get(0).getType() != Type.DATATYPE) {
				MessagePrinter.printMessage(MessageType.ERROR, "invalid data type " + "'" + tokens.get(0).getVal() + "'");
			}

			dataType = tokens.remove(0).getVal();

			if (dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar")) {
				if (tokens.get(0).getType() != Type.L_PAREN) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected '(' afetr " + tokens.get(0).getVal());
				}
				dataType += tokens.remove(0).getVal();

				if (tokens.get(0).getType() != Type.INTEGER) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected a integer value inside ()");
				}

				dataType += tokens.remove(0).getVal();

				if (tokens.get(0).getType() != Type.R_PAREN) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected closing ) after " + tokens.get(0).getVal());
				}

				dataType += tokens.remove(0).getVal();
			}

			while (tokens.get(0).getType() == Type.CONSTRAINT) {
				String contraint = tokens.remove(0).getVal();
				switch (contraint) {
					case "notnull":
						notNull = true;
						break;
					case "primarykey":
						primaryKey = true;
						notNull = true;
						unique = true;
						break;
					case "unique":
						unique = true;
						break;
					default:
						break;
				}
			}

			attributes.add(new AttributeSchema(attributeName, dataType, notNull, primaryKey, unique));

			if (tokens.get(0).getType() != Type.COMMA && tokens.get(0).getType() != Type.R_PAREN) {
				MessagePrinter.printMessage(MessageType.ERROR, "Expected a ','");
			}

			if (tokens.get(0).getType() == Type.COMMA) {
				tokens.remove(0);
			}
		}

		tokens.remove(0); // remove closing )

		if (tokens.get(0).getType() != Type.SEMICOLON) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected a ';'");
		}
		tokens.remove(0);

		TableSchema schema = new TableSchema(tableName);
		schema.setAttributes(attributes);
		return schema;
	}

	public static String parseDropTable(ArrayList<Token> tokens) throws Exception {
		tokens.remove(0); // remove drop token
		tokens.remove(0); // remove table token

		if (tokens.get(0).getType() != Type.NAME) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected a table name");
		}
		return tokens.get(0).getVal();
	}

	/**
	 *
	 * @param tokens the alter statement passed in
	 * @returns altervals, a hashmap of strings with the following values:
	 *          tableName: Name of the table to be altered
	 *          adddrop: "add" | "drop" defining the function to complete
	 *          attriname: Name of the attribute to be altered
	 *          type: type of the new attribute, defaults will be checked
	 *          parser-side
	 *          deflt: default value to change all existing rows in the table to for
	 *          the new attri.
	 *          will be passed as a string regardless of type.
	 * @throws Exception
	 */
	public static HashMap<String, String> parseAlterTable(ArrayList<Token> tokens) throws Exception {
		HashMap<String, String> altervals = new HashMap<>();
		String tableName = "";
		String adddrop = "";
		String attriname = "";
		String type = "null";
		String deflt = "";
		String isDeflt = "false";
		String charType = "";
		tokens.remove(0); // remove alter token
		tokens.remove(0); // remove table token

		if (tokens.get(0).getType() != Type.NAME) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected table name");
		}

		tableName = tokens.remove(0).getVal();

		if (!tokens.get(0).getVal().equalsIgnoreCase("drop") && !tokens.get(0).getVal().equalsIgnoreCase("add")) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected 'drop' or 'add' kwyword");
		}

		adddrop = tokens.remove(0).getVal();

		if (tokens.get(0).getType() != Type.NAME) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected attribute name");
		}

		attriname = tokens.remove(0).getVal();

		if (adddrop.equalsIgnoreCase("add")) {
			if (tokens.get(0).getType() != Type.DATATYPE) {
				MessagePrinter.printMessage(MessageType.ERROR, "Expected a valid data type for " + attriname);
			}
			type = tokens.remove(0).getVal();

			if (type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
				if (tokens.get(0).getType() != Type.L_PAREN) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected '(' afetr " + tokens.get(0).getVal());
				}
				type += tokens.remove(0).getVal();

				if (tokens.get(0).getType() != Type.INTEGER) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected a integer value inside ()");
				}

				type += tokens.remove(0).getVal();

				if (tokens.get(0).getType() != Type.R_PAREN) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected closing ) after " + tokens.get(0).getVal());
				}

				type += tokens.remove(0).getVal();
			}

			if (tokens.get(0).getType() != Type.SEMICOLON && !tokens.get(0).getVal().equalsIgnoreCase("default")) {
				MessagePrinter.printMessage(MessageType.ERROR, "Expected either a ';' or 'default' keyword");
			}

			if (tokens.get(0).getVal().equalsIgnoreCase("default")) {
				tokens.remove(0);
				isDeflt = "true";

				if (tokens.get(0).getType() != Type.INTEGER && tokens.get(0).getType() != Type.DOUBLE
						&& tokens.get(0).getType() != Type.BOOLEAN && tokens.get(0).getType() != Type.NULL
						&& tokens.get(0).getType() != Type.STRING) {
					MessagePrinter.printMessage(MessageType.ERROR, "Expected a valid value for the default");
				}

				deflt = tokens.remove(0).getVal();
			}
		}

		if (tokens.get(0).getType() != Type.SEMICOLON) {
			MessagePrinter.printMessage(MessageType.ERROR, "Expected a ';'");
		}
		tokens.remove(0);
		altervals.put("tableName", tableName);
		altervals.put("adddrop", adddrop);
		altervals.put("attriname", attriname);
		altervals.put("type", type);
		altervals.put("deflt", deflt);
		altervals.put("isDeflt", isDeflt);
		return altervals;
	}
}
