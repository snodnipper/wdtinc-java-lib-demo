package uk.os.wdtinc.demo.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class VectorTileUtil {

    private VectorTileUtil(){}

    public static byte[] getUncompressedVectorTileFrom(byte[] bytes) throws IOException {

        byte[] result;
        if (isGZIPStream(bytes)) {
            result = getUncompressedFromGzip(bytes);
        } else {
            result = bytes;
        }
        return result;
    }

    public static byte[] getCompressedVectorTileFrom(byte[] bytes) throws IOException {
        byte[] result;
        if (isGZIPStream(bytes)) {
            result = bytes;
        } else {
            result = getCompressedAsGzip(bytes);
        }
        return result;
    }

    // Source: https://www.javacodegeeks.com/2015/01/working-with-gzip-and-compressed-data.html
    public static boolean isGZIPStream(byte[] bytes) {
        return bytes[0] == (byte) GZIPInputStream.GZIP_MAGIC
                && bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >>> 8);
    }

    public static byte[] getCompressedAsGzip(byte[] uncompressed) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzipper = new GZIPOutputStream(out);
        gzipper.write(uncompressed);
        gzipper.close();
        out.close();
        return out.toByteArray();
    }

    public static byte[] getUncompressedFromGzip(byte[] compressed) throws IOException {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GZIPInputStream gzipper = new GZIPInputStream(new ByteArrayInputStream(compressed));

        int len;
        while ((len = gzipper.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }

        gzipper.close();
        out.close();
        return out.toByteArray();
    }
}
