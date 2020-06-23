package com.greyuhu.hbase.hbase;

import com.greyuhu.hbase.entity.Department;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseScan {
    public static final String TABLE_NAME = "greyhuhu:dept3";

    public HBaseScan() {
        //提前链接hbase数据库
        HBaseUtil.getScanner(TABLE_NAME);
    }

    /**
     * 获得全部的部门
     *
     * @throws IOException
     */
    public List<Department> getAllDept() throws IOException {
        ResultScanner resultScanner = HBaseUtil.getScanner(TABLE_NAME);
        List<Department> departments = new ArrayList<>();
        assert resultScanner != null;
        Result result = resultScanner.next();
        while (result != null) {
            departments.add(scannerToResult(result));
            result = resultScanner.next();
        }
        return departments;
    }


    /**
     * 根据rowKey获得部门
     *
     * @return
     * @throws IOException
     */
    public Department getDeptByRowKey(String rowKey) throws IOException {
        Result result = HBaseUtil.getRow(TABLE_NAME, rowKey);
        assert result != null;
        return scannerToResult(result);
    }


    /**
     * 根据rowkey获得其子部门
     *
     * @param rowKey
     * @return
     * @throws IOException
     */
    public List<Department> getAllSubDeptByRowKey(String rowKey) throws IOException {
        List<Department> departments = new ArrayList<>();
        Result result = HBaseUtil.getRow(TABLE_NAME, rowKey);
        List<String> subRowKeys = new ArrayList<>();
        if (result != null) {
            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("subdept"));
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
//                将子部门存入list
                subRowKeys.add(Bytes.toString(entry.getKey()));
            }
        } else {
            return null;
        }
//        查询子部门
        for (String s : subRowKeys) {
            Result temp = HBaseUtil.getRow(TABLE_NAME, s);
            if (temp != null) {
                Department department = scannerToResult(temp);
                if (department != null)
                    departments.add(department);
            } else
                return null;
        }
        return departments;
    }

    /**
     * 把一个result变成Department
     *
     * @param result
     * @return
     */
    public Department scannerToResult(Result result) {
        try {
            String rowkey = Bytes.toString(result.getRow());
            String base_name = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")));
            String base_f_pid = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("f_pid")));
            List<String> stringList = new ArrayList<>();
            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("subdept"));
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                stringList.add(Bytes.toString(entry.getKey()));
            }
            return new Department(rowkey, base_name, base_f_pid, stringList);
        } catch (Exception e) {
            System.out.println(1100 + ":报错了 ");
        }
        return null;
    }
}
