package com.alibaba.otter;
import java.net.InetSocketAddress;
import java.util.List;


import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.consts.ES;
import com.alibaba.otter.dao.CourseES;


public class App {

    private static boolean runningFlag = true;

    public static void main(String args[]) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "", "");

        CourseES courseES = new CourseES(ES.restHighLevelClient());

        int batchSize = 1000;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runningFlag = false;
            connector.disconnect();
            courseES.disconnect();
            System.out.println("\n[INFO] All connections closed successfully.");

        }));

        System.out.println("[INFO] YangYu-es-updater startup successfully.");
        try {
            connector.connect();
            connector.subscribe(".*\\.course");
            connector.rollback();
            while (runningFlag) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    handleEntries(message.getEntries(),courseES);
                }
                if(runningFlag)
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

        } finally {
            connector.disconnect();
        }
    }

    private static void handleEntries(List<Entry> entrys, CourseES courseES ) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("[INFO] binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                  //  printColumn(rowData.getBeforeColumnsList());
                    courseES.deleteCourse(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                  //  printColumn(rowData.getAfterColumnsList());
                    courseES.addCourse(rowData.getAfterColumnsList());
                } else {
//                    System.out.println("-------&gt; before");
//                    printColumn(rowData.getBeforeColumnsList());
//                    System.out.println("-------&gt; after");
//                    printColumn(rowData.getAfterColumnsList());
                    courseES.updateCourse(rowData.getAfterColumnsList());
                }
            }
            System.out.println("[INFO] Process done.");
        }
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

}

