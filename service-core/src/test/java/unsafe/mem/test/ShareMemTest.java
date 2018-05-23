package unsafe.mem.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ShareMemTest {
    public static String fileName = "/home/wpy/tmp/mem.share";
    private static RandomAccessFile RAFile;
    public static MappedByteBuffer mapBuff;
    private static int size = 1024 * 1024 * 1024;


    static {
        try {
            RAFile = new RandomAccessFile(fileName, "rw");
            mapBuff = RAFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < size/2; i++) {
            mapBuff.putChar(i,(char)i);
        }
    }
}
