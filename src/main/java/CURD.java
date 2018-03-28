
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
        Table table = conn.getTable(TableName.valueOf("tbl1"));
        Put put = new Put(Bytes.toBytes("row-1"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("greet"), Bytes.toBytes("Hello"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("person"), Bytes.toBytes("John"));
        table.put(put);
        table.close();

    }

}
