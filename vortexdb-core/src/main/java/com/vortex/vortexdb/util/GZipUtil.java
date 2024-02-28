
package com.vortex.vortexdb.util;

import com.vortex.common.util.Bytes;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Reference from https://dzone.com/articles/how-compress-and-uncompress
 */
public final class GZipUtil {

    private static final int BUF_SIZE = (int) (4 * Bytes.KB);

    public static String md5(String input){
        return DigestUtils.md5Hex(input);
    }

    public static BytesBuffer compress(byte[] data) {
        int estimateSize = data.length >> 3;
        BytesBuffer output = BytesBuffer.allocate(estimateSize);

        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();
        byte[] buffer = new byte[BUF_SIZE];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            output.write(buffer, 0, count);
        }
        output.forReadWritten();
        return output;
    }

    public static BytesBuffer decompress(byte[] data) {
        int estimateSize = data.length << 3;
        BytesBuffer output = BytesBuffer.allocate(estimateSize);

        Inflater inflater = new Inflater();
        inflater.setInput(data);
        byte[] buffer = new byte[BUF_SIZE];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buffer);
                output.write(buffer, 0, count);
            } catch (DataFormatException e) {
                throw new BackendException("Failed to decompress", e);
            }
        }
        output.forReadWritten();
        return output;
    }
}
