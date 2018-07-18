package git.wpy.format.vector;

/**
 * Created by wangpengyu6 on 18-7-18.
 */
public enum OffHeapDataType {
    /*byte(bool); short; int; long; float; double; binary*/
    INT_8(0),
    INT_16(1),
    INT_32(2),
    INT_64(3),
    FLOAT(4),
    DOUBLE(5),
    BINARY(6);

    int value;

    OffHeapDataType(int value) {
        this.value = value;
    }
}
