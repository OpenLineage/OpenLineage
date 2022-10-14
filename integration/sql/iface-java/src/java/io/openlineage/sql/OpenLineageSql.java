package io.openlineage.sql;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

public final class OpenLineageSql {
    public static native SqlMeta parse(List<String> sql, String dialect, String defaultSchema) throws RuntimeException;
    public static SqlMeta parse(List<String> sql, String dialect) throws RuntimeException { return parse(sql, dialect, null); }
    public static SqlMeta parse(List<String> sql) throws RuntimeException { return parse(sql, null, null); }
    
    public static native String provider();

    private static void loadNativeLibrary(String libName) throws IOException {
        String fullName = "io/openlineage/sql/" + libName;

        URL url = OpenLineageSql.class.getResource("/" + fullName);
        if (url == null) {
            throw new IOException("Library not found in resources.");
        }

        File tmpDir = Files.createTempDirectory("native-lib").toFile();
        tmpDir.deleteOnExit();
        File nativeLib = new File(tmpDir, libName);
        nativeLib.deleteOnExit();

        try (InputStream in = url.openStream()) {
            Files.copy(in, nativeLib.toPath());
        }

        System.load(nativeLib.getAbsolutePath());
    }

    static {
        String libName = "libopenlineage_sql_java";
        if (SystemUtils.IS_OS_MAC_OSX) {
            libName += ".dylib";
        } else if (SystemUtils.IS_OS_LINUX) {
            libName += ".so";
        } else {
            throw new RuntimeException("Cannot link native library: unsupported OS");
        }

        try {
            loadNativeLibrary(libName);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Error extracting native library '%s': %s", libName, e.getMessage()));
        }
    }
}