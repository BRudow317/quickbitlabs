// MyJavaUtil/Simple.java
package MyJavaUtil;

/**
 * Simple stuff so I don't have to rewrite it every time.
 */

public class Simple {
    
    public static String truncate(String str, int maxLength) {
        if (str == null) return "null";
        return str.length() > maxLength ? str.substring(0, maxLength - 3) + "..." : str;
    }















}
