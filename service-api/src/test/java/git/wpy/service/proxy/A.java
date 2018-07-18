package git.wpy.service.proxy;

import java.io.Serializable;

public class A implements Serializable {

    public void print(){
        System.out.println("AAAAA");
    }

    public void run(){
        print();
    }
}
