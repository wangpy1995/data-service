package git.wpy.bean;

import java.util.List;

public class ColumnFamily {
    private String name;
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
}
