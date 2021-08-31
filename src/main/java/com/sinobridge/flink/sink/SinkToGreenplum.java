package com.sinobridge.flink.sink;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.sinobridge.flink.entity.Fission;
import com.sinobridge.flink.entity.FissionGroup;
import com.sinobridge.flink.entity.FissionGroupMember;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class SinkToGreenplum extends RichSinkFunction<List<Fission>> {

    private Connection conn = null;
    private PreparedStatement pstmt=null;
    private Properties prop;

    public SinkToGreenplum(Properties prop) {
        this.prop=prop;
    }

    @Override
    public void open(Configuration parameters)  {

        //注册驱动
        //Class.forName("org.postgresql.Driver");
        try {
            super.open(parameters);
            /*//注册驱动
            Class.forName(driverName);
            //创建数据库连接
            conn = DriverManager.getConnection(url,username,password);*/

            //使用druid管理连接池
            DataSource dataSource = DruidDataSourceFactory.createDataSource(prop);
            conn = dataSource.getConnection();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(List<Fission> fissionList, Context context)  {
        if (fissionList.size() != 0 ) {
            //创建prepareStatement对象
            String sql = null;
            try {
                for (Fission fission: fissionList) {
                    if (fission instanceof FissionGroup) {
                        sql = "insert into fission_group(id,group_code) values(?,?)";
                        pstmt = conn.prepareStatement(sql);
                        FissionGroup fg = ((FissionGroup) fission);
                        Integer id = fg.getId();
                        String group_code = fg.getGroup_code();
                        pstmt.setInt(1,id);
                        pstmt.setString(2,group_code);
                        pstmt.addBatch(); //将sql加入到批处理
                    } else if (fission instanceof FissionGroupMember) {
                        sql = "insert into fission_group_member(id,group_id) values(?,?)";
                        pstmt = conn.prepareStatement(sql);
                        FissionGroupMember fgm = ((FissionGroupMember) fission);
                        Integer id = fgm.getId();
                        String group_id = fgm.getGroup_id();
                        pstmt.setInt(1,id);
                        pstmt.setString(2,group_id);
                        pstmt.addBatch(); //将sql加入到批处理
                    }
                }

                //执行批任务
                pstmt.executeBatch();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (pstmt != null) {
            pstmt.close();
        }
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
        super.close();
    }
}
