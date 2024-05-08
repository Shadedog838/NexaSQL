package Parser;

public class Token {
    private Type type;
    private String val;
    private String prior;
    private String latter;
    private int charsize;

    public Token(Type type, String val){
        this.type = type;
        this.val = val;
    }
    //overloaded constructors my beloved
    //intended only for CHAR and VARCHAR Type
    //charsize is the number, e.g. 32 in varchar(32)
    public Token(Type type, String val, int charsize){
        this.type = type;
        this.val = val;
        this.charsize = charsize;
    }
    //overloaded constructors my beloved
    //intended for IDDOUBLE Type
    //handles tokens in the form of prior.latter
    public Token(Type type, String val, String prior, String latter){
        this.type = type;
        this.val = val;
        this.prior = prior;
        this.latter = latter;
    }
    public Type getType(){
        return type;
    }

    //getVal gets the true, properly typed value of the token, when applicable. good for inserting or comparing data.
    //use toString for String values.
    public String getVal(){
        return val;
    }

    //bit of a misnomer since val is a string by default but I'm a sucker for tradition.
    public String toString(){
        return val;
    }
    //probably useful shorthand, esp for parsing chars
    public int getSize(){
        return val.length();
    }

    //saving you the pain of splitting the value later!
    public String getPrior(){
        return prior;
    }
    public String getLatter(){
        return latter;
    }

}
