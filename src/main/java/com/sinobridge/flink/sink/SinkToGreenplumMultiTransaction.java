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
import java.util.*;

public class SinkToGreenplumMultiTransaction extends RichSinkFunction<List<Fission>> {

    private Connection conn = null;
    private PreparedStatement pstmt=null;
    private DataSource dataSource = null;
    private Properties prop;

    public SinkToGreenplumMultiTransaction(Properties prop) {
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
            if ( dataSource == null ) {
                dataSource = DruidDataSourceFactory.createDataSource(prop);
            }
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
            List<FissionGroup>  fissionGroupList = new ArrayList<>();
            List<FissionGroupMember>  fissionGroupMemberList= new ArrayList<>();


            //遍历fissionList，切分成FissionGroup和FissionGroupMember2个List
            for (Fission fission: fissionList ) {
                if (fission instanceof FissionGroup) {
                    FissionGroup fg = ((FissionGroup) fission);
                    fissionGroupList.add(fg);
                } else if (fission instanceof FissionGroupMember) {
                    FissionGroupMember fgm = ((FissionGroupMember) fission);
                    fissionGroupMemberList.add(fgm);
                }
            }


            long startTime = 0;
            long endTime = 0;
            int transactionId = 0;
            //针对fissionGroupList做批操作
            if (fissionGroupList.size() > 0 ) {
                try {
                    startTime = new Date().getTime();
                    transactionId = new Random().nextInt(100000);
                    System.out.println(transactionId + " fissionGroup Sink id: " + transactionId +" 流程开始时间：" + new Date());
                    //conn.setAutoCommit(false);
                    sql = "insert into fission_group(id,group_code) values(?,?)";
                    pstmt = conn.prepareStatement(sql);
                    for (FissionGroup fg: fissionGroupList) {
                        Long id = fg.getId();
                        String group_code = fg.getGroup_code();
                        pstmt.setLong(1,id);
                        pstmt.setString(2,group_code);
                        pstmt.addBatch(); //将sql加入到批处理
                    }
                    pstmt.executeBatch();
                    //conn.commit();
                    endTime = new Date().getTime();
                    System.out.println("Sink id: " +transactionId + " "+ new Date() +" 本次写fission_group条数：" + fissionGroupList.size() + "，运行时间："+ (endTime- startTime) + " 毫秒");
                } catch (SQLException e) {
                    /*try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }*/
                    e.printStackTrace();
                }
            }

            //针对fissionGroupMemberList做批操作
            if (fissionGroupMemberList.size() >0 ) {
                try {
                    startTime = new Date().getTime();
                    transactionId = new Random().nextInt(100000);
                    System.out.println("fissionGroupMember Sink id: " + transactionId +" 流程开始时间：" + new Date());
                    //conn.setAutoCommit(false);
                    sql = "insert into fission_group_member(id,group_id) values(?,?)";
                    pstmt = conn.prepareStatement(sql);
                    for (FissionGroupMember fgm: fissionGroupMemberList) {
                        Long id = fgm.getId();
                        String group_id = fgm.getGroup_id();
                        pstmt.setLong(1,id);
                        pstmt.setString(2,group_id);
                        pstmt.addBatch(); //将sql加入到批处理
                    }
                    pstmt.executeBatch();
                    //conn.commit();
                    endTime = new Date().getTime();
                    System.out.println("Sink id: " +transactionId + " "+ new Date() + " 本次写fission_group_member条数：" + fissionGroupMemberList.size() + "，运行时间："+ (endTime- startTime) + " 毫秒");
                } catch (SQLException e) {
                    /*try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }*/
                    e.printStackTrace();
                }
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
