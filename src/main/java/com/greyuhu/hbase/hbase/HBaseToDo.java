package com.greyuhu.hbase.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseToDo {
    public static final String TABLE_NAME = "greyhuhu:dept3";

    /**
     * 测试连接
     *
     * @throws IOException
     */
    public void test() throws IOException {
        System.out.println("测试连接。。。。");
        TableName[] list = HBaseConn.getHBaseConn().getAdmin().listTableNames();
        this.printTableNames(list);
        System.out.println("连接成功。。。");
    }

    /**
     * 2 使用Java代码实现以下场景所需要的HBase数据库表dept，并增加列族base和subdept
     *
     * @throws IOException
     */
    public void secondGoal() throws IOException {
        String[] family = new String[2];
        family[0] = "base";
        family[1] = "subdept";
//        创建表
        System.out.println("创建表...");
        if (HBaseUtil.createTable(TABLE_NAME, family)) {
            this.printTableNames(null);
            System.out.println("创建成功...");
        } else {
            System.out.println("创建失败");
        }
    }

    /**
     * 3 使用Java代码循环向dept表插入以部门ID作为行键的满足业务场景描述的数据不少于200条记录
     *
     * @throws IOException
     */
    public void thirdGoal() throws IOException {
        String topRowKey = "0_001";
//        添加顶部的公司
        HBaseUtil.putRow(TABLE_NAME, topRowKey, "base", "name", "公司");
        String[] mediumDeps = new String[10];
//        添加十个中级部门
        for (int i = 1; i <= 10; i++) {
            String rowKey = i < 10 ? "1_00" + i : "1_0" + i;
            mediumDeps[i - 1] = rowKey;
//            添加名称
            HBaseUtil.putRow(TABLE_NAME, rowKey, "base", "name", "中级部门" + i);
//            添加父级部门
            HBaseUtil.putRow(TABLE_NAME, rowKey, "base", "f_pid", topRowKey);
//            添加父级部门的子部门
            HBaseUtil.putRow(TABLE_NAME, topRowKey, "subdept", rowKey, "中级部门" + i);
        }
//添加200的下级部门
        for (int i = 1; i <= 200; i++) {
            int j = (i - 1) / 20;
            String f_pid = mediumDeps[j];
            String rowKey;
            if (i < 10)
                rowKey = "2_00" + i;
            else if (i < 100)
                rowKey = "2_0" + i;
            else
                rowKey = "2_" + i;
//            添加名称
            HBaseUtil.putRow(TABLE_NAME, rowKey, "base", "name", "下级部门" + i);
//            添加中级为父部门
            HBaseUtil.putRow(TABLE_NAME, rowKey, "base", "f_pid", f_pid);
//            添加到中级部门的子部门
            HBaseUtil.putRow(TABLE_NAME, f_pid, "subdept", rowKey, "下级部门" + i);
        }
    }

    /**
     * 4．使用Java代码完成以下查询操作
     */
    public void fourthGoal() {
        /*
         *  1.查询所有一级部门(没有上级部门的部门)
         */
        getTopDept();

        /*
         * 2.已知rowkey，查询该部门的所有(直接)子部门信息
         */
//        获得指定rowkey ：0_001的子部门
        getSubDeptByFID("0_001");

        /*
        	3.已知rowkey：0_001，向该部门增加一个子部门
         */
        addSubDeptByRowKey();

        /*
         * 4.已知rowkey：1_010（且该部门存在子部门2_181-2_200），删除该部门信息，该部门所有(直接)子部门被调整到其他部门：1_009中
         */
        getSubDeptByFID("1_009");
        delRowAndChangeInto();
    }

    /**
     * 已知rowkey：1_010（且该部门存在子部门2_181-2_200），删除该部门信息，该部门所有(直接)子部门被调整到其他部门：1_009中
     */
    public void delRowAndChangeInto() {
//        获得子部门
        Result result2 = HBaseUtil.getRow(TABLE_NAME, "1_010");
        List<String> subRowKeys2 = new ArrayList<>();
        if (result2 != null) {
            Map<byte[], byte[]> familyMap = result2.getFamilyMap(Bytes.toBytes("subdept"));
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
//                将子部门rowkey存入list
                subRowKeys2.add(Bytes.toString(entry.getKey()));
            }
        } else {
            System.out.println("未查到信息");
        }
//        添加list
        List<Put> putList = new ArrayList<>();
        //            设置新的父部门的rowkey
        String base_f_pid = "1_009";
//        将子部门调整到其他部门
        for (String rowKey : subRowKeys2) {
//            获得子部门
            Result result3 = HBaseUtil.getRow(TABLE_NAME, rowKey);
            assert result3 != null;
            String rowkey = Bytes.toString(result3.getRow());
            String base_name = Bytes.toString(result3.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")));
//            生成更新子部门的put：父部门改为新的父部门
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("f_pid"), Bytes.toBytes(base_f_pid));
//            生成更新父部门的put：加入新的子部门
            Put put1 = new Put(Bytes.toBytes(base_f_pid));
            put1.addColumn(Bytes.toBytes("subdept"), Bytes.toBytes(rowkey), Bytes.toBytes(base_name));
            putList.add(put);
            putList.add(put1);
        }
//        更新子部门的父部门的id以及父部门的子部门
        HBaseUtil.putRows(TABLE_NAME, putList);
//        删除原有的部门
        HBaseUtil.deleteRow(TABLE_NAME, "1_010");
//        查看结果
        System.out.println("\n\n\n\n");
        getSubDeptByFID("1_009");
    }

    /**
     * 已知rowkey：0_001，向该部门增加一个子部门
     */
    public void addSubDeptByRowKey() {
        //    添加子部门名称
        HBaseUtil.putRow(TABLE_NAME, "text_1", "base", "name", "测试插入部门");
//            添加子部门的父部门
        HBaseUtil.putRow(TABLE_NAME, "text_1", "base", "f_pid", "0_001");
//            添加父部门的子部门
        HBaseUtil.putRow(TABLE_NAME, "0_001", "subdept", "text_1", "测试插入部门");
//        查询打印结果
        Result result1 = HBaseUtil.getRow(TABLE_NAME, "text_1");
        if (result1 != null)
            HBaseUtil.printResult(result1);
        else
            System.out.println("未查到信息");
    }

    /**
     * 1.查询所有一级部门(没有上级部门的部门)
     */
    public void getTopDept() {
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("base"), Bytes.toBytes("f_pid"),
                CompareFilter.CompareOp.EQUAL, "".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(scvf);
//        查询
        ResultScanner results = HBaseUtil.getScanner(TABLE_NAME, filterList);
//        打印结果
        if (results != null)
            results.forEach(HBaseUtil::printResult);
        else {
            System.out.println("未查到信息");
        }
    }

    /**
     * 获得当前rowkey的子部门
     *
     * @param row
     */
    public void getSubDeptByFID(String row) {
        Result result = HBaseUtil.getRow(TABLE_NAME, row);
        List<String> subRowKeys = new ArrayList<>();
        if (result != null) {
            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("subdept"));
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
//                将子部门存入list
                subRowKeys.add(Bytes.toString(entry.getKey()));
            }
        } else {
            System.out.println("未查到信息");
        }
//        查询子部门
        for (String rowKey : subRowKeys) {
            Result temp = HBaseUtil.getRow(TABLE_NAME, rowKey);
            if (temp != null)
                HBaseUtil.printResult(temp);
            else
                System.out.println("未查到信息");
        }
    }

    /**
     * 打印表表名
     *
     * @param tableNames
     * @throws IOException
     */
    public void printTableNames(TableName[] tableNames) throws IOException {
        System.out.println("当前所有表为。。。");
        if (tableNames == null) {
            tableNames = HBaseConn.getHBaseConn().getAdmin().listTableNames();
        }
        for (TableName tableName : tableNames) {
            System.out.println(tableName.toString());
        }

    }


}
