package git.wpy.format.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by wangpengyu6 on 18-7-18.
 */
public class OffHeapColumnVector implements WritableByteChannel, ReadableByteChannel {

    /**
     * 指针
     */
    private long columnPtr;

    public OffHeapColumnVector(OffHeapDataType offHeapDataType, long capacity) {
        this.columnPtr = OffHeapUtils.allocateColumn(offHeapDataType.value, capacity);
    }

    public void setNullAt(long rowId) {
        OffHeapUtils.setNullAt(columnPtr, rowId);
    }

    //INT8
    public void setByte(long rowId, byte value) {
        OffHeapUtils.setByte(columnPtr, rowId, value);
    }

    public void setBool(long rowId, boolean value) {
        OffHeapUtils.setBool(columnPtr, rowId, value);
    }

    //INT16
    public void setShort(long rowId, short value) {
        OffHeapUtils.setShort(columnPtr, rowId, value);
    }

    public void setChar(long rowId, char value) {
        OffHeapUtils.setChar(columnPtr, rowId, value);
    }

    //INT32
    public void setInt(long rowId, int value) {
        OffHeapUtils.setInt(columnPtr, rowId, value);
    }

    //INT64
    public void setLong(long rowId, long value) {
        OffHeapUtils.setLong(columnPtr, rowId, value);
    }

    public void setFloat(long rowId, float value) {
        OffHeapUtils.setFloat(columnPtr, rowId, value);
    }

    public void setDouble(long rowId, double value) {
        OffHeapUtils.setDouble(columnPtr, rowId, value);
    }

    public void setByteArray(long rowId, byte[] value) {
        OffHeapUtils.setByteArray(columnPtr, rowId, value);
    }

    public void setByteArray(long rowId, ByteBuffer value) {
        OffHeapUtils.setByteArray(columnPtr, rowId, value);
    }

    public void setString(long rowId, String value) {
        OffHeapUtils.setString(columnPtr, rowId, value);
    }

    /**
     * @param rowId
     * @param values
     * @param count
     * @apiNote 只支持基本类型
     */
    public void bulkPut(long rowId, byte[] values, int count) {
        OffHeapUtils.bulkPut(columnPtr, rowId, values, count);
    }

    public void bulkRead(ByteBuffer dst) {
        OffHeapUtils.bulkRead(columnPtr, 0, dst);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        OffHeapUtils.bulkRead(columnPtr, 0, dst);
        dst.rewind();
        return dst.capacity();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        src.rewind();
        if (src.remaining() > 0)
            OffHeapUtils.bulkPut(columnPtr, 0, src, 1);
        return src.capacity();
    }

    @Override
    public boolean isOpen() {
        return columnPtr != 0;
    }

    @Override
    public void close() throws IOException {

    }
}
