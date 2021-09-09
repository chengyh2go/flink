package com.sinobridge.flink.dbops;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

public class CustomTimerTask extends TimerTask {

    private Properties prop;
    private Connection conn = null;
    private PreparedStatement pstmt = null;
    public CustomTimerTask(Properties prop) {
        this.prop = prop;
    }

    @Override
    public void run() {
        System.out.println( new Date() + " reduce逻辑启动");
        String driverName = prop.getProperty("driverClassName");
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        String sql;
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, username, password);

            //对fission_group_state做reduce操作
            sql = "truncate fission_state_temp;";
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();

            sql = "insert into fission_state_temp(partition_value,offset_value) select partition_value,max(offset_value) from fission_group_state group by partition_value;";
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();

            sql = "begin;\n" +
                    "LOCK TABLE fission_group_state IN ACCESS EXCLUSIVE MODE;\n" +
                    "truncate fission_group_state;\n" +
                    "insert into fission_group_state(partition_value,offset_value) select partition_value,offset_value from fission_state_temp;\n" +
                    "commit;";
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();

            //对fission_group_member_state做reduce操作
            sql = "truncate fission_state_temp;";
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();

            sql = "insert into fission_state_temp(partition_value,offset_value) select partition_value,max(offset_value) from fission_group_state group by partition_value;";
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();

            sql = "begin;\n" +
                    "LOCK TABLE fission_group_member_state IN ACCESS EXCLUSIVE MODE;\n" +
                    "truncate fission_group_member_state;\n" +
                    "insert into fission_group_member_state(partition_value,offset_value) select partition_value,offset_value from fission_state_temp;\n" +
                    "commit;";
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();
            System.out.println( new Date() + " reduce逻辑完成");

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null ) {
                    pstmt.close();
                }
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
