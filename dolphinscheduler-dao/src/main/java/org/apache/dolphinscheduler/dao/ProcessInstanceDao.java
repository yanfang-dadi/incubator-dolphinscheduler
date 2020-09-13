package org.apache.dolphinscheduler.dao;

import org.apache.dolphinscheduler.dao.datasource.ConnectionFactory;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.mapper.ProcessInstanceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;

@Component
public class ProcessInstanceDao extends AbstractBaseDao{
    public static final Logger logger = LoggerFactory.getLogger(ProcessInstanceDao.class);

    @Autowired
    private ProcessInstanceMapper processInstanceMapper;

    public ProcessInstance queryProcessInstanceById(int id){
        ProcessInstance instance = processInstanceMapper.queryDetailById(id);
        return instance;
//        ProcessInstance processInstance = new ProcessInstance();
//
//        String sql = String.format("SELECT process_definition_id from t_ds_process_instance tdpi where id = ?");
//        ResultSet rs = null;
//        PreparedStatement pstmt = null;
//        try {
//            pstmt = conn.prepareStatement(sql);
//            pstmt.setInt(1,id);
//            rs = pstmt.executeQuery();
//
//            while (rs.next()){
//                Integer pdi = rs.getInt(1);
//                processInstance.setProcessDefinitionId(pdi);
//            }
//
//        } catch (Exception e) {
//            logger.error(e.getMessage(),e);
//            throw new RuntimeException("sql: " + sql, e);
//        } finally {
//            ConnectionUtils.releaseResource(rs, pstmt, conn);
//        }
//
//        processInstance.setId(id);
//
//        return processInstance;
    }

    @Override
    protected void init() {
        processInstanceMapper = ConnectionFactory.getInstance().getMapper(ProcessInstanceMapper.class);
    }
}
