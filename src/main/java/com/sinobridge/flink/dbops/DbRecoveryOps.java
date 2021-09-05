package com.sinobridge.flink.dbops;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

public class DbRecoveryOps {

    private Properties prop= null;

    public DbRecoveryOps(Properties prop) {
        this.prop = prop;
    }

    public HashMap<Integer,Long> fetchFissionGroupPartitionAndOffset() throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap<Integer, Long> hashMap = new HashMap<>();
        //使用DriverManager创建连接
        //注册驱动
        String driverName = prop.getProperty("driverClassName");
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        //创建数据库连接
        String checkSql = "select partition_value,max(offset_value) as max_offset from fission_group group by partition_value;";
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, username, password);
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
            if (rs != null ) {
                rs.close();
            }
            if (pstmt != null ) {
                pstmt.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }

        }
        return hashMap;
    }

    public HashMap<Integer,Long> fetchFissionGroupMemberPartitionAndOffset() throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap<Integer, Long> hashMap = new HashMap<>();
        //使用DriverManager创建连接
        //注册驱动
        String driverName = prop.getProperty("driverClassName");
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        //创建数据库连接
        String checkSql = "select partition_value,max(offset_value) as max_offset from fission_group_member group by partition_value;";
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, username, password);
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
            if (rs != null ) {
                rs.close();
            }
            if (pstmt != null ) {
                pstmt.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }

        }
        return hashMap;
    }
}
