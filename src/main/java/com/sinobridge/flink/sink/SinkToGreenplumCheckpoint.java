package com.sinobridge.flink.sink;

import com.sinobridge.flink.dbops.CustomDbUtils;
import com.sinobridge.flink.entity.Fission;
import com.sinobridge.flink.entity.FissionGroup;
import com.sinobridge.flink.entity.FissionGroupMember;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class SinkToGreenplumCheckpoint extends RichSinkFunction<List<Fission>>   {

    private CustomDbUtils dbUtils;
    private Connection conn = null;
    private PreparedStatement fgpSTMT =null;
    private PreparedStatement fgMpStmt =null;
    private PreparedStatement fgpStateSTMT =null;
    private PreparedStatement fgmStatePStmt =null;
    //private Properties prop;
    private HashMap<Integer,Long> fission_group_stateMap;
    private HashMap<Integer,Long> fission_group_member_stateMap;

    //private DataSource dataSource = null;
    //private transient ListState<List<Fission>> checkpointedState = null;

    public SinkToGreenplumCheckpoint(CustomDbUtils dbUtils,
                                     HashMap<Integer,Long> fission_group_stateMap,
                                     HashMap<Integer,Long> fission_group_member_stateMap
                                     ) {
        this.dbUtils=dbUtils;
        this.fission_group_stateMap = fission_group_stateMap;
        this.fission_group_member_stateMap = fission_group_member_stateMap;

    }

    @Override
    public void open(Configuration parameters)  throws Exception {
        super.open(parameters);

        //使用DriverManager创建连接
        //注册驱动
        /*String driverName = prop.getProperty("driverClassName");
        Class.forName(driverName);
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        //创建数据库连接
        conn = DriverManager.getConnection(url,username,password);*/

        //使用druid创建连接池
        /*if ( dataSource == null ) {
            dataSource = DruidDataSourceFactory.createDataSource(prop);
        }
        conn = dataSource.getConnection();*/
        conn = dbUtils.getConnection();
    }

    @Override
    public void invoke(List<Fission> fissionListInput, Context context)  {

        //创建prepareStatement对象
        String fgSql = "insert into fission_group(id,group_code) values(?,?)";
        String fgmSql = "insert into fission_group_member(id,group_id) values(?,?)";
        String fgStateSql = "insert into fission_group_state(partition_value,offset_value) values(?,?)";
        String fgmStateSql = "insert into fission_group_member_state(partition_value,offset_value) values(?,?)";

        try {
            fgpSTMT = conn.prepareStatement(fgSql);
            fgMpStmt = conn.prepareStatement(fgmSql);
            fgpStateSTMT = conn.prepareStatement(fgStateSql);
            fgmStatePStmt = conn.prepareStatement(fgmStateSql);
            conn.setAutoCommit(false);

            for (Fission fission : fissionListInput) {
                if (fission instanceof FissionGroup) {
                    FissionGroup fg = ((FissionGroup) fission);
                    Long id = fg.getId();
                    String group_code = fg.getGroup_code();
                    Long offset_value = fg.getOffset_value();
                    Integer partition_value = fg.getPartition_value();
                    fgpSTMT.setLong(1, id);
                    fgpSTMT.setString(2, group_code);
                    fgpSTMT.addBatch(); //将sql加入到批处理

                    //如果fission_group_stateMap不包含partition_value这个key，就做put操作
                    if (!fission_group_stateMap.containsKey(partition_value)) {
                        fission_group_stateMap.put(partition_value, offset_value);
                    } //如果包含partition_value这个key，就比较offset_value，保存最大值
                    else if (fission_group_stateMap.get(partition_value) < offset_value) {
                        fission_group_stateMap.put(partition_value, offset_value);
                    }

                } else if (fission instanceof FissionGroupMember) {
                    FissionGroupMember fgm = ((FissionGroupMember) fission);
                    Long id = fgm.getId();
                    String group_id = fgm.getGroup_id();
                    Long offset_value = fgm.getOffset_value();
                    Integer partition_value = fgm.getPartition_value();
                    fgMpStmt.setLong(1, id);
                    fgMpStmt.setString(2, group_id);
                    fgMpStmt.addBatch(); //将sql加入到批处理

                    //如果fission_group_member_stateMap不包含partition_value这个key，就做put操作
                    if (!fission_group_member_stateMap.containsKey(partition_value)) {
                        fission_group_member_stateMap.put(partition_value, offset_value);
                    } //如果包含partition_value这个key，就比较offset_value，保存最大值
                    else if (fission_group_member_stateMap.get(partition_value) < offset_value) {
                        fission_group_member_stateMap.put(partition_value, offset_value);
                    }
                }
            }

            fgpSTMT.executeBatch();
            fgMpStmt.executeBatch();

            //将partition和offset信息写入数据库：fission_group_state和fission_group_member_state
            dbUtils.insertPartitionAndOffset(fission_group_stateMap,fgpStateSTMT);
            dbUtils.insertPartitionAndOffset(fission_group_member_stateMap,fgmStatePStmt);

            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (fgpSTMT != null) {
            fgpSTMT.close();
        }
        if (fgMpStmt != null) {
            fgMpStmt.close();
        }
        if (fgpStateSTMT != null) {
            fgpStateSTMT.close();
        }
        if (fgmStatePStmt != null) {
            fgmStatePStmt.close();
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
