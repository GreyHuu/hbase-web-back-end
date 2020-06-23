package com.greyuhu.hbase.entity;

import java.util.List;

public class Department {
    //rowkey
    private String rowKey;
    //部门名称
    private String base_name;
    //父部门的rowkey
    private String base_f_pid;
    //子部门的rowkey
    private List<String> subDept;

    public Department(String rowKey, String base_name, String base_f_pid, List<String> subDept) {
        this.rowKey = rowKey;
        this.base_name = base_name;
        this.base_f_pid = base_f_pid;
        this.subDept = subDept;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getBase_name() {
        return base_name;
    }

    public void setBase_name(String base_name) {
        this.base_name = base_name;
    }

    public String getBase_f_pid() {
        return base_f_pid;
    }

    public void setBase_f_pid(String base_f_pid) {
        this.base_f_pid = base_f_pid;
    }

    public List<String> getSubDept() {
        return subDept;
    }

    public void setSubDept(List<String> subDept) {
        this.subDept = subDept;
    }
}
