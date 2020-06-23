package com.greyuhu.hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 连接HBase
 */
public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration; //hbase配置
    private static Connection connection; //hbase connection

    private HBaseConn() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
//                hbase
                configuration.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
//                zookeeper
                configuration.set("hbase.zookeeper.quorum", "hbase");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获得HBase的连接对象
     *
     * @return
     */
    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 根据表名获得表
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}