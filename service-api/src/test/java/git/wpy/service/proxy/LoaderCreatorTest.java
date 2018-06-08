package git.wpy.service.proxy;

import git.wpy.service.annotation.LoaderCreator;

import java.io.*;
import java.net.URL;

@LoaderCreator(loaderType = "nothing")
public class LoaderCreatorTest {
    public void createLoader() {
        System.out.println("create loader.");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        B b = new B();
        A a = (A)b;

        URL file = b.getClass().getResource("/clz");
//        OutputStream out =  new FileOutputStream(file.getFile());
//
//        ObjectOutputStream output = new ObjectOutputStream(out);
//        output.writeObject(a);
//        output.flush();
//        output.close();

        ObjectInputStream input = new ObjectInputStream(new FileInputStream(file.getFile()));
        A a0 = (A)input.readObject();
        System.out.println(a0.getClass().getName());
    }
}