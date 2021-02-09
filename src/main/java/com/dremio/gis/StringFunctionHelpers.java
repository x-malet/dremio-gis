package com.dremio.gis;

import org.apache.arrow.memory.ArrowBuf;

public class StringFunctionHelpers {
    public static String toStringFromUTF8(int start, int end, ArrowBuf buffer) {
        byte[] buf = new byte[end - start];
        buffer.getBytes(start, buf, 0, end - start);
        return new String(buf, com.google.common.base.Charsets.UTF_8);
    }
}
