package git.wpy.format.vector;

import java.nio.ByteBuffer;

/**
 * Created by wangpengyu6 on 18-7-18.
 */
public class OffHeapUtils {

    /**
     * 申请一列数据对应的空间
     *
     * @param offHeapDataType 数据类型
     * @param capacity        数据总数
     * @return
     */
    static native long allocateColumn(int offHeapDataType, long capacity);

    static native void setNullAt(long ptr, long rowId);

    //INT8
    static native void setByte(long ptr, long rowId, byte value);

    static void setBool(long ptr, long rowId, boolean value) {
        setByte(ptr, rowId, value ? (byte) 1 : (byte) 0);
    }

    //INT16
    static native void setShort(long ptr, long rowId, short value);

    static void setChar(long ptr, long rowId, char value) {
        setShort(ptr, rowId, (short) value);
    }

    //INT32
    static native void setInt(long ptr, long rowId, int value);

    //INT64
    static native void setLong(long ptr, long rowId, long value);

    static native void setFloat(long ptr, long rowId, float value);

    static native void setDouble(long ptr, long rowId, double value);

    static native void setByteArray(long ptr, long rowId, byte[] value);

    static native void setByteArray(long ptr, long rowId, ByteBuffer value);

    static void setString(long ptr, long rowId, String value) {
        setByteArray(ptr, rowId, value.getBytes());
    }

    static native void bulkPut(long ptr, long rowId, ByteBuffer values, int count);

    static native void bulkPut(long ptr, long rowId, byte[] values, int count);

    static native void bulkPut(long ptr, long rowId, short[] values, int count);

    static native void bulkPut(long ptr, long rowId, char[] values, int count);

    static native void bulkPut(long ptr, long rowId, int[] values, int count);

    static native void bulkPut(long ptr, long rowId, long[] values, int count);

    static native void bulkPut(long ptr, long rowId, float[] values, int count);

    static native void bulkPut(long ptr, long rowId, double[] values, int count);

    static native void bulkRead(long ptr,long rowId,ByteBuffer dst);


    static native void freeColumn(long ptr);
}
