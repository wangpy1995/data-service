package git.wpy.bean;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ColumnFamily {
    private static String name;
    private List<Qualifier> qualifierList;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Qualifier> getQualifierList() {
        return qualifierList;
    }

    public void setQualifierList(List<Qualifier> qualifierList) {
        this.qualifierList = qualifierList;
    }

    /*static {
        InputStream in = ColumnFamily.class.getResourceAsStream("/xx");
        try {
            System.out.println("init name.");
            byte[] buff = new byte[in.available()];
            in.read(buff);
            name=new String(buff);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                in.close();
                in=null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }*/

    public static void main(String[] args) {
        System.out.println("00000000000000");
    }
}
