package git.wpy.service.proxy;

import git.wpy.service.annotation.LoaderCreator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.URL;

@LoaderCreator(loaderType = "nothing")
public class LoaderCreatorTest {
    public void createLoader() {
        System.out.println("create loader.");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        B b = new B();
        A a = (A) b;


        byte[] x = {'2', '0', '1', '8', '0', '3', '1', '1', '1', '1'};
        byte[] y = {'2', '0', '1', '8', '0', '6', '3', '1', '1', '1', '4', '5'};

        byte[][] res = Bytes.split(x, y, true, 5);
        int len = x.length < y.length ? x.length : y.length;
        for (int i = 0; i < len; i++) {

        }

        for (int i = 0; i < res.length; i++) {
            System.out.println(Bytes.toString(res[i]));
        }
        System.exit(0);
        URL file = b.getClass().getResource("/clz");
//        OutputStream out =  new FileOutputStream(file.getFile());
//
//        ObjectOutputStream output = new ObjectOutputStream(out);
//        output.writeObject(a);
//        output.flush();
//        output.close();

        ObjectInputStream input = new ObjectInputStream(new FileInputStream(file.getFile()));
        A a0 = (A) input.readObject();
        System.out.println(a0.getClass().getName());
    }
}

class A {
}

class B extends A {
}