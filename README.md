# CSCI421---Group-2

Authors: Alen Van, Shandon Mith, Carlos Hargrove, Chen Lin, Erika Jeffers

Compilation:
    In order to compile the java program, navigate to the root directory and perform:
        javac Main.java
    
    In order to run the program afterwards, perform the following:
        java Main <dbLoc> <page_size> <buffer_size>

Structure:
    Entry Point:
        Main.java
        Database
        UserInterface
        Parser
        QueryExecutor
        Catalog
        StorageManager
    Bottom Most

Main.Java
    Entry point of the program. Initializes database and starts up the user interface.

Database
    Initializes the database if it does not exist yet.
    If the database already exists then reads in the catalog.
    Upon program shutdown, it safely closes the program by saving all pages in the buffer as well as the catalog.

UserInterface
    Reads in user input and passes it to the Parser.
    From there, if the input was a display, display either the entire database schema or a single table's schema, otherwise, call the appropriate method in the QueryExecutor.

Parser
    Tokenizes the user input and verifies the input is correct.

QueryExecutor
    Verifies the input data types and constraints, then passes the information of the inputted query to the correct method in either Catalog or StorageManager.

Catalog
    Stores and manipulates the database schema.

StorageManager
    The back end of the program that handles database manipulation such as inserting a record, creating a table, dropping a table, or altering a table.
    The StorageManager also handles any and all reading and writing to hardware in the program.