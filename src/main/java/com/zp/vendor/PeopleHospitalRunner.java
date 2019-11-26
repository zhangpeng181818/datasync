package com.zp.vendor;

import com.zp.utils.StringUtil;
import com.alibaba.datax.common.element.Record;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
@Component
public class PeopleHospitalRunner {

    private PeopleHospitalReader orderReader;

    private PeopleHospitalWriter orderWriter;

    private PeopleHospitalReader lisReader;

    private PeopleHospitalWriter lisWriter;

    private PeopleHospitalReader patientReader;

    private PeopleHospitalWriter patientWriter;

    private PeopleHospitalReader patientTransferReader;

    private PeopleHospitalWriter patienTransfertWriter;

    private String classPath = StringUtil.getCurrentClasspath();

    public PeopleHospitalRunner() {
        init();
        initPatientConfig();
//        initOrderConfig();
        initLisConfig();
        initPatientTransferConfig();
    }

    public void startSyncHisPatientsJob() throws SQLException {
        List<Record> patientList = patientReader.queryPatientList();
        patientWriter.startWrite(patientList);
    }

    public void startSyncHisOrderJob() throws SQLException {
        List<Integer> queryValidIds = orderWriter.queryValidIds("order_no", "order_status", "在用");
        startWrite(queryValidIds);
        int order_no = orderWriter.queryColumnMaxValue("order_no");
        List<Record> records = orderReader.incrementQueryOrders(String.valueOf(order_no));
        orderWriter.startWrite(records);
    }

    public void startSyncLisItemsResultJob() throws SQLException {
        List<Integer> queryValidIds = patientWriter.queryValidIds("patient_id", "status", "住院");
        if (queryValidIds.isEmpty()) {
            return;
        }
        List<Record> recordList = lisReader.queryValidLisItemsResult("ReviewDate", "ReviewTime", "PatIndex", StringUtil.replace(queryValidIds.toString()));
        lisWriter.startWrite(recordList);
    }

    public void startSyncHisPatientTransfertJob() throws SQLException {
        List<Record> records = patientTransferReader.queryPatientTransList();
        if (records.isEmpty()){
            return;
        }
        patienTransfertWriter.startWrite(records);
    }

    private void init() {
        System.setProperty("datax.home", classPath);
    }

    private void initOrderConfig() {
        orderReader = new PeopleHospitalReader(classPath + "job/order.json");
        orderWriter = new PeopleHospitalWriter(classPath + "job/order.json");
    }

    private void initLisConfig() {
        lisReader = new PeopleHospitalReader(classPath + "job/lis.json");
        lisWriter = new PeopleHospitalWriter(classPath + "job/lis.json");
    }

    private void initPatientConfig() {
        patientReader = new PeopleHospitalReader(classPath + "job/patient.json");
        patientWriter = new PeopleHospitalWriter(classPath + "job/patient.json");
    }

    private void initPatientTransferConfig() {
        patientTransferReader = new PeopleHospitalReader(classPath + "job/patient_transfer.json");
        patienTransfertWriter = new PeopleHospitalWriter(classPath + "job/patient_transfer.json");
    }

    private void startWrite(List<Integer> recordList) {
        long startTime = System.currentTimeMillis();
        Iterator<Integer> recordIterator = recordList.iterator();

        List<Integer> writeBuffer = new ArrayList<>(StringUtil.batchSize);

        List<Record> list;
        try {
            Integer record;
            while (recordIterator.hasNext()) {
                record = recordIterator.next();
                writeBuffer.add(record);
                if (writeBuffer.size() >= StringUtil.batchSize) {
                    list = orderReader.queryValidOrders("order_no", StringUtil.replace(writeBuffer.toString()));
                    orderWriter.startWrite(list);

                    writeBuffer.clear();
                }
            }
            if (!writeBuffer.isEmpty()) {
                list = orderReader.queryValidOrders("order_no", StringUtil.replace(writeBuffer.toString()));
                orderWriter.startWrite(list);
                writeBuffer.clear();
            }
        } catch (Exception var10) {
            log.debug("{}", var10.getMessage());
        } finally {
            writeBuffer.clear();
            log.debug("共写入：{} 条记录,耗时：{} ms", recordList.size(), (System.currentTimeMillis() - startTime));
        }
    }
}
