
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CURD {
    public static void main(String[] args) throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin =conn.getAdmin();
        TableName  tblname = TableName.valueOf("tbl1");
        if (admin.tableExists(tblname)) {
            System.out.println(tblname + " exits.");
        }else{
            HTableDescriptor hTableDesc = new HTableDescriptor(tblname);
            hTableDesc.addFamily(new HColumnDescriptor("cf1"));
            admin.createTable(hTableDesc);
        }
//      PUT
        Table table = conn.getTable(TableName.valueOf("tbl1"));
        List<Put> puts = new ArrayList<Put>();
        for(int i = 0; i< 100; i++){
            Put put = new Put(Bytes.toBytes("row-"+i));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("greet"), Bytes.toBytes("Hello"));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("person"), Bytes.toBytes("John"));
            puts.add(put);
        }
        table.put(puts);
//    GET
        Get get = new Get(Bytes.toBytes("row-10"));
        get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("greet"));
        Result result = table.get(get);
        byte[] val = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("greet"));
        System.out.println("Cell Value of row-10:" + Bytes.toString(val));

//        SCAN
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("greet"));
        scan.setStartRow(Bytes.toBytes("row-1"));
        scan.setStopRow(Bytes.toBytes("row-20"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result res: scanner){
            System.out.println("Row Value" + res);
        }


        table.close();

    }

}
