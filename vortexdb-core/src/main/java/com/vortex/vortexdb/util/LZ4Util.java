
package com.vortex.vortexdb.util;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import net.jpountz.lz4.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class LZ4Util {

    protected static final float DEFAULT_BUFFER_RATIO = 1.5f;

    public static BytesBuffer compress(byte[] bytes, int blockSize) {
        return compress(bytes, blockSize, DEFAULT_BUFFER_RATIO);
    }

    public static BytesBuffer compress(byte[] bytes, int blockSize,
                                       float bufferRatio) {
        float ratio = bufferRatio <= 0.0F ? DEFAULT_BUFFER_RATIO : bufferRatio;
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        int initBufferSize = Math.round(bytes.length / ratio);
        BytesBuffer buf = new BytesBuffer(initBufferSize);
        LZ4BlockOutputStream lz4Output = new LZ4BlockOutputStream(
                                         buf, blockSize, compressor);
        try {
            lz4Output.write(bytes);
            lz4Output.close();
        } catch (IOException e) {
            throw new BackendException("Failed to compress", e);
        }
        /*
         * If need to perform reading outside the method,
         * remember to call forReadWritten()
         */
        return buf;
    }

    public static BytesBuffer decompress(byte[] bytes, int blockSize) {
        return decompress(bytes, blockSize, DEFAULT_BUFFER_RATIO);
    }

    public static BytesBuffer decompress(byte[] bytes, int blockSize,
                                         float bufferRatio) {
        float ratio = bufferRatio <= 0.0F ? DEFAULT_BUFFER_RATIO : bufferRatio;
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        int initBufferSize = Math.min(Math.round(bytes.length * ratio),
                                      BytesBuffer.MAX_BUFFER_CAPACITY);
        BytesBuffer buf = new BytesBuffer(initBufferSize);
        LZ4BlockInputStream lzInput = new LZ4BlockInputStream(bais,
                                                              decompressor);
        int count;
        byte[] buffer = new byte[blockSize];
        try {
            while ((count = lzInput.read(buffer)) != -1) {
                buf.write(buffer, 0, count);
            }
            lzInput.close();
        } catch (IOException e) {
            throw new BackendException("Failed to decompress", e);
        }
        /*
         * If need to perform reading outside the method,
         * remember to call forReadWritten()
         */
        return buf;
    }
}
