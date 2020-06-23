package com.greyuhu.hbase.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 操作HBase 的工具类
 */
public class HBaseUtil {
    /**
     * 创建表
     *
     * @param tableName 创建表的表名称
     * @param cfs       列簇的集合
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     * @return
     */
    public static boolean deleteTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 获得一个表的原始数据
     *
     * @param tableName 表名
     */
    public static void getNoDealData(String tableName) {
        try {
            Table table = HBaseConn.getHBaseConn().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for (Result result : resutScanner) {
                System.out.println("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowkey
     * @param cfName    列族的名称
     * @param qualifer  列的名称
     * @param data      值
     * @return
     */
    public static boolean putRow(String tableName, String rowkey, String cfName, String qualifer, String data) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifer), Bytes.toBytes(data));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 批量插入数据
     *
     * @param tableName 表的名称
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try (Table table = HBaseConn.getTable(tableName)) {
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 根据Rowkey查询单条数据
     *
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result getRow(String tableName, String rowkey) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowkey));
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 带有过滤器的插入数据
     *
     * @param tableName
     * @param rowkey
     * @param filterList
     * @return
     */
    public static Result getRow(String tableName, String rowkey, FilterList filterList) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowkey));
            get.setFilter(filterList);
            Result result = table.get(get);
            printResult(result);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * scan扫描数据，
     *
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            return getResults(table, scan);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    //查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col) {
        try {
            Table table = HBaseConn.getHBaseConn().getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                result = Bytes.toString(resByte);
            } else {
                result = "查询结果不存在";
                return result;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "出现异常";
    }

    /**
     * scan 检索数据，控制startrow，stoprow 注意包括startrow 不包括stoprow，
     *
     * @param tableName
     * @param startKey
     * @param stopKey
     * @return
     */
    public static ResultScanner getScanner(String tableName, String startKey, String stopKey) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(stopKey));
            return getResults(table, scan);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * scan 检索数据，通过传入FilterList进行查询
     *
     * @param tableName
     * @param filterList
     * @return
     */
    public static ResultScanner getScanner(String tableName, FilterList filterList) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return getResults(table, scan);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除行
     *
     * @param tableName
     * @param rowkey
     * @return
     */
    public static boolean deleteRow(String tableName, String rowkey) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列簇
     *
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteColumnFamily(String tableName, String cfName) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(cfName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列
     *
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteQualifier(String tableName, String rowkey, String cfName, String qualiferName) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualiferName));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 获得扫描的结果
     *
     * @param table
     * @param scan
     * @return
     * @throws IOException
     */
    private static ResultScanner getResults(Table table, Scan scan) throws IOException {
        scan.setCaching(1000);
        return table.getScanner(scan);
    }

    /**
     * 打印结果
     *
     * @param result
     */
    public static void printResult(Result result) {
        try {
            System.out.println("rowkey == " + Bytes.toString(result.getRow()));
            System.out.println("base:name == " + Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("name"))));
            System.out.println("base:f_pid == " + Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("f_pid"))));
            System.out.print("subdept:  subdept_id ==");
            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("subdept"));
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                System.out.print(Bytes.toString(entry.getKey()) + " ");
            }
            System.out.println();
        } catch (Exception ignored) {
        }
    }
}