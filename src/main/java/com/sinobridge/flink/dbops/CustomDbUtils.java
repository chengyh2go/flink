package com.sinobridge.flink.dbops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class CustomDbUtils implements Serializable {

    private Properties prop;

    public CustomDbUtils(Properties prop) {
        this.prop = prop;
    }

    //打开连接
    public Connection getConnection() throws ClassNotFoundException, SQLException {
        //使用DriverManager创建连接
        //注册驱动
        String driverName = prop.getProperty("driverClassName");
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        Class.forName(driverName);
        return DriverManager.getConnection(url, username, password);
    }

    //关闭连接
    private static void closeConnection(ResultSet rs, Statement stmt, Connection conn) {
        try {
            if (rs != null ) {
                rs.close();
            }
            if (stmt != null ) {
                stmt.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //插入数据到分区和offset状态记录表
    public void insertPartitionAndOffset(HashMap<Integer,Long> hashMap,PreparedStatement pstmt) throws Exception  {

        Set<Map.Entry<Integer, Long>> entries = hashMap.entrySet();
        for (Map.Entry<Integer, Long> entry: entries){
            pstmt.setInt(1,entry.getKey());
            pstmt.setLong(2,entry.getValue());
            pstmt.addBatch();
        }
        pstmt.executeBatch();

    }

    //获取分区和offset信息，在程序刚启动时运行，用以判断从哪个offset开始消费kafka数据
    public  HashMap<Integer,Long> fetchGroupPartitionAndOffset(String checkSql)  {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap<Integer, Long> hashMap = new HashMap<>();

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(checkSql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                long max_offset = rs.getLong("max_offset");
                int partition_value = rs.getInt("partition_value");
                hashMap.put(partition_value, max_offset);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection(rs,pstmt,conn);
        }
        return hashMap;
    }







}
