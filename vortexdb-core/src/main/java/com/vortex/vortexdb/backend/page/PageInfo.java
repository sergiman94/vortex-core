
package com.vortex.vortexdb.backend.page;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import com.vortex.common.iterator.Metadatable;
import com.vortex.common.util.Bytes;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.StringEncoding;

import java.util.Iterator;

public final class PageInfo {

    public static final String PAGE = "page";
    public static final String PAGE_NONE = "";

    private int offset;
    private String page;

    public PageInfo(int offset, String page) {
        E.checkArgument(offset >= 0, "The offset must be >= 0");
        E.checkNotNull(page, "page");
        this.offset = offset;
        this.page = page;
    }

    public void increase() {
        this.offset++;
        this.page = PAGE_NONE;
    }

    public int offset() {
        return this.offset;
    }

    public void page(String page) {
        this.page = page;
    }

    public String page() {
        return this.page;
    }

    @Override
    public String toString() {
        return StringEncoding.encodeBase64(this.toBytes());
    }

    public byte[] toBytes() {
        byte[] pageState = PageState.toBytes(this.page);
        int length = 2 + BytesBuffer.INT_LEN + pageState.length;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.writeInt(this.offset);
        buffer.writeBytes(pageState);
        return buffer.bytes();
    }

    public static PageInfo fromString(String page) {
        byte[] bytes;
        try {
            bytes = StringEncoding.decodeBase64(page);
        } catch (Exception e) {
            throw new VortexException("Invalid page: '%s'", e, page);
        }
        return fromBytes(bytes);
    }

    public static PageInfo fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return new PageInfo(0, PAGE_NONE);
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            int offset = buffer.readInt();
            byte[] pageState = buffer.readBytes();
            String page = PageState.toString(pageState);
            return new PageInfo(offset, page);
        } catch (Exception e) {
            throw new VortexException("Invalid page: '0x%s'",
                                    e, Bytes.toHex(bytes));
        }
    }

    public static PageState pageState(Iterator<?> iterator) {
        E.checkState(iterator instanceof Metadatable,
                     "Invalid paging iterator: %s", iterator.getClass());
        Object page = ((Metadatable) iterator).metadata(PAGE);
        E.checkState(page instanceof PageState,
                     "Invalid PageState '%s'", page);
        return (PageState) page;
    }

    public static String pageInfo(Iterator<?> iterator) {
        E.checkState(iterator instanceof Metadatable,
                     "Invalid paging iterator: %s", iterator.getClass());
        Object page = ((Metadatable) iterator).metadata(PAGE);
        return page == null ? null : page.toString();
    }
}
