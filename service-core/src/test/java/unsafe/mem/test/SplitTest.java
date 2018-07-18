package unsafe.mem.test;

import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangpengyu6 on 18-6-14.
 */
public class SplitTest {

    private static long maxSize = (long) 1;

    public static void split(byte[] start, byte[] stop, long size, List<byte[]> list) {
        if (size > maxSize) {
            byte[] left = TableInputFormatBase.getSplitKey(start, stop, true);
            size /= 2;
            split(start, left, size, list);
            list.add(left);
            split(left, stop, size, list);
        }
    }

    @Test
    public void test() {
        byte[] start = {'2', '0', '1', '8', '0', '3', Byte.MIN_VALUE};
        byte[] stop = {'2', '0', '1', '8', '0', '6', Byte.MAX_VALUE};
        List<byte[]> list = new ArrayList<>();
        list.add(start);
        split(start, stop, 8, list);
        list.add(stop);
        list.forEach(bytes -> System.out.println(Bytes.toString(bytes)));
    }
}
