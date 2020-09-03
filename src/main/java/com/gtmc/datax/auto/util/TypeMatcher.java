package com.gtmc.datax.auto.util;

public class TypeMatcher
{
    public static String hiveType2FileColType(String hiveType)
    {
        hiveType = hiveType.toUpperCase();
        String result = "";
        if ((hiveType.equals("TINYINT")) || (hiveType.equals("SMALLINT")) || (hiveType.equals("INT")) || (hiveType.equals("BIGINT"))) {
            result = "Long";
        } else if ((hiveType.equals("FLOAT")) || (hiveType.equals("DOUBLE"))) {
            result = "Double";
        } else if ((hiveType.equals("STRING")) || (hiveType.equals("VARCHAR")) || (hiveType.equals("CHAR"))) {
            result = "String";
        } else if (hiveType.equals("BOOLEAN")) {
            result = "Boolean";
        } else if ((hiveType.equals("DATE")) || (hiveType.equals("TIMESTAMP"))) {
            result = "Date";
        }
        return result;
    }
}
