package io.openlineage.sql;

import java.util.List;

public final class OpenLineageSql {
    public static native SqlMeta parse(List<String> sql, String dialect, String defaultSchema) throws RuntimeException;
    public static SqlMeta parse(List<String> sql, String dialect) throws RuntimeException { return parse(sql, dialect, null); }
    public static SqlMeta parse(List<String> sql) throws RuntimeException { return parse(sql, null, null); }
    
    public static native String provider();
    
    static {
        System.loadLibrary("openlineage_sql_java");
    }
}