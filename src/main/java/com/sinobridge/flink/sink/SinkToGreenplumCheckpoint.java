package com.sinobridge.flink.sink;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.sinobridge.flink.entity.Fission;
import com.sinobridge.flink.entity.FissionGroup;
import com.sinobridge.flink.entity.FissionGroupMember;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class SinkToGreenplumCheckpoint extends RichSinkFunction<List<Fission>>   {

    private Connection conn = null;
    private PreparedStatement fgPstmt=null;
    private PreparedStatement fgmPstmt=null;
    private Properties prop;
    //private DataSource dataSource = null;
    //private transient ListState<List<Fission>> checkpointedState = null;

    public SinkToGreenplumCheckpoint(Properties prop) {
        this.prop=prop;
    }

    @Override
    public void open(Configuration parameters)  throws Exception {
        super.open(parameters);

        //使用DriverManager创建连接
        //注册驱动
        String driverName = prop.getProperty("driverClassName");
        Class.forName(driverName);
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        //创建数据库连接
        conn = DriverManager.getConnection(url,username,password);

        //使用druid创建连接池
        /*if ( dataSource == null ) {
            dataSource = DruidDataSourceFactory.createDataSource(prop);
        }
        conn = dataSource.getConnection();*/
    }

    @Override
    public void invoke(List<Fission> fissionListInput, Context context)  {

        //创建prepareStatement对象
        String fgSql = null;
        String fgmSql = null;

        fgSql = "insert into fission_group(id,group_code,offset_value,partition_value) values(?,?,?,?)";
        fgmSql = "insert into fission_group_member(id,group_id,offset_value,partition_value) values(?,?,?,?)";

        try {
            fgPstmt = conn.prepareStatement(fgSql);
            fgmPstmt = conn.prepareStatement(fgmSql);
            conn.setAutoCommit(false);

            for (Fission fission : fissionListInput) {
                if (fission instanceof FissionGroup) {
                    FissionGroup fg = ((FissionGroup) fission);
                    Long id = fg.getId();
                    String group_code = fg.getGroup_code();
                    Long offset_value = fg.getOffset_value();
                    Integer partition_value = fg.getPartition_value();
                    fgPstmt.setLong(1, id);
                    fgPstmt.setString(2, group_code);
                    fgPstmt.setLong(3,offset_value);
                    fgPstmt.setInt(4,partition_value);
                    fgPstmt.addBatch(); //将sql加入到批处理
                } else if (fission instanceof FissionGroupMember) {
                    FissionGroupMember fgm = ((FissionGroupMember) fission);
                    Long id = fgm.getId();
                    String group_id = fgm.getGroup_id();
                    Long offset_value = fgm.getOffset_value();
                    Integer partition_value = fgm.getPartition_value();
                    fgmPstmt.setLong(1, id);
                    fgmPstmt.setString(2, group_id);
                    fgmPstmt.setLong(3,offset_value);
                    fgmPstmt.setInt(4,partition_value);
                    fgmPstmt.addBatch(); //将sql加入到批处理
                }
            }

            fgPstmt.executeBatch();
            fgmPstmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (fgPstmt != null) {
            fgPstmt.close();
        }
        if (fgmPstmt != null) {
            fgmPstmt.close();
        }
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
        super.close();
    }


    //CheckpointedFunction
    /*@Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 将之前的Checkpoint清理
        System.out.println(new Date() + " snapshotState被调用");
        checkpointedState.clear();
        System.out.println(new Date() + " snapshotState要写的fissionList的总数" + fissionList.size());
        checkpointedState.add(fissionList);
        System.out.println(new Date() + " snapshotState完成");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println(new Date() + " initializeState被调用");
        ListStateDescriptor<List<Fission>> listStateDescriptor =
                new ListStateDescriptor<List<Fission>>("fission-list",
                        TypeInformation.of(new TypeHint<List<Fission>>() {
                        }));
        checkpointedState = context.getOperatorStateStore().getListState(listStateDescriptor);
        System.out.println(new Date() + " 从checkpointedState获取到的state信息：" + checkpointedState.get());
        if (context.isRestored()) {
            System.out.println(new Date() + " 开始从checkpoint中获取state");
            for ( List<Fission> fissionListFromCheckpointState : checkpointedState.get()) {
                fissionList.addAll(fissionListFromCheckpointState);
            }
            System.out.println(fissionList);
        }
    }*/
}
