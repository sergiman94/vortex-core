
package com.vortex.vortexdb.backend.id;

import com.vortex.vortexdb.backend.id.Id.IdType;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;

public final class IdUtil {

    public static String writeStoredString(Id id) {
        String idString;
        switch (id.type()) {
            case LONG:
            case STRING:
            case UUID:
                idString = IdGenerator.asStoredString(id);
                break;
            case EDGE:
                idString = EdgeId.asStoredString(id);
                break;
            default:
                throw new AssertionError("Invalid id type " + id.type());
        }
        return id.type().prefix() + idString;
    }

    public static Id readStoredString(String id) {
        IdType type = IdType.valueOfPrefix(id);
        String idContent = id.substring(1);
        switch (type) {
            case LONG:
            case STRING:
            case UUID:
                return IdGenerator.ofStoredString(idContent, type);
            case EDGE:
                return EdgeId.parseStoredString(idContent);
            default:
                throw new IllegalArgumentException("Invalid id: " + id);
        }
    }

    public static Object writeBinString(Id id) {
        int len = id.edge() ? BytesBuffer.BUF_EDGE_ID : id.length() + 1;
        BytesBuffer buffer = BytesBuffer.allocate(len).writeId(id);
        buffer.forReadWritten();
        return buffer.asByteBuffer();
    }

    public static Id readBinString(Object id) {
        BytesBuffer buffer = BytesBuffer.wrap((ByteBuffer) id);
        return buffer.readId();
    }

    public static String writeString(Id id) {
        String idString = id.asString();
        StringBuilder sb = new StringBuilder(1 + idString.length());
        sb.append(id.type().prefix()).append(idString);
        return sb.toString();
    }

    public static Id readString(String id) {
        IdType type = IdType.valueOfPrefix(id);
        String idContent = id.substring(1);
        switch (type) {
            case LONG:
                return IdGenerator.of(Long.parseLong(idContent));
            case STRING:
            case UUID:
                return IdGenerator.of(idContent, type == IdType.UUID);
            case EDGE:
                return EdgeId.parse(idContent);
            default:
                throw new IllegalArgumentException("Invalid id: " + id);
        }
    }

    public static String writeLong(Id id) {
        return String.valueOf(id.asLong());
    }

    public static Id readLong(String id) {
        return IdGenerator.of(Long.parseLong(id));
    }

    public static String escape(char splitor, char escape, String... values) {
        int length = values.length + 4;
        for (String value : values) {
            length += value.length();
        }
        StringBuilder escaped = new StringBuilder(length);
        // Do escape for every item in values
        for (String value : values) {
            if (escaped.length() > 0) {
                escaped.append(splitor);
            }

            if (value.indexOf(splitor) == -1) {
                escaped.append(value);
                continue;
            }

            // Do escape for current item
            for (int i = 0, n = value.length(); i < n; i++) {
                char ch = value.charAt(i);
                if (ch == splitor) {
                    escaped.append(escape);
                }
                escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    public static String[] unescape(String id, String splitor, String escape) {
        /*
         * Note that the `splitor`/`escape` maybe special characters in regular
         * expressions, but this is a frequently called method, for faster
         * execution, we forbid the use of special characters as delimiter
         * or escape sign.
         * The `limit` param -1 in split method can ensure empty string be
         * splited to a part.
         */
        String[] parts = id.split("(?<!" + escape + ")" + splitor, -1);
        for (int i = 0; i < parts.length; i++) {
            parts[i] = StringUtils.replace(parts[i], escape + splitor,
                                           splitor);
        }
        return parts;
    }
}
