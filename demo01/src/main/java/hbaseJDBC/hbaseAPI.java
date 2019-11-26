package hbaseJDBC;

import java.net.InetAddress;
import java.util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.*;

/**
 * Created by zhangxinyu6 on 2018/11/6.
 */

public class hbaseAPI {


//调试用main函数
    public static void main(String args[]) throws Exception {
        //BasicConfigurator.configure();
        //PropertyConfigurator.configure("src/log4j.properties");
    	Connection con=hbaseAPI.ConnectHbase("10.66.175.215","hdh215");
    	
    	//hbaseAPI.CreateTable(con,"zxy","c1,c2,c3");
    	//hbaseAPI.DropTable(con,"zxy");
//    	hbaseAPI.InsertData(con,"test:test","1911181234567890","cf","testId","233");
    	//hbaseAPI.SelectDataAll(con,"zxy","1");
    	//System.out.println(hbaseAPI.SelectDataAll(con,"HUMAN_INFO","0001_face009_2378651d-736d-4051-9e61-d3d9f401bb7e"));
    	//System.out.println(hbaseAPI.SelectColumnData(con,"HUMAN_INFO","0001_face009_2378651d-736d-4051-9e61-d3d9f401bb7e","info","error_code"));
    	//String a=hbaseAPI.SelectColumnData(con,"HUMAN_INFO","0001_face009_2378651d-736d-4051-9e61-d3d9f401bb7e","info","error_code");
//    	hbaseAPI.Describe(con,"HUMAN_INFO");
//    	hbaseAPI.Truncate(con,"hik_identity_index_info");
//    	hbaseAPI.rowCount(con, "XVEHICLE:BAYONET_VEHICLEALARM");
//    	hbaseAPI.ScanTable(con, "BAYONET_VEHICLEPASS");
//    	System.out.println(hbaseAPI.TimeRangeAll(con, "HIK_SMART_METADATA", "1564576430000","1558925414000").size());
//    	System.out.println(hbaseAPI.TimeRangeAll(con, "XBODY:HIK_SMART_METADATA", "1564992193073").size());
    	Table table=con.getTable(TableName.valueOf("test:test"));
        Put put=new Put(Bytes.toBytes("2019101912345"));
        int i=1234;
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("testId"), Bytes.toBytes(i));//列族(family)，列(Qualifier),数据
        table.put(put);
        System.out.println("插入成功" );
        table.close();
    	
    	
    	
    	hbaseAPI.CloseCon(con,"10.66.175.215");
    }
//*/

    /**
     * 连接，自动修改hosts文件加入IP hostname。
     * @param zookeeper地址
     * @param hostname，不填使用hdh格式
     * @param hosts文件路径，不填使用win7路径
     */
    public static Connection ConnectHbase(String IP,String hostname,String path) throws Exception{
        Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.OFF);
        logger.addAppender(new ConsoleAppender(new PatternLayout("%r [%t] %p %c %x - %m%n")));
        
        //检查IP和hostname是否对应，否则修改
        InetAddress inetAddress=InetAddress.getByName(IP);
        if(inetAddress.getHostName().toString().equals(IP)) {
        	String IpHost = IP+" "+hostname;
        	File file =new File(path);
        	FileWriter fileWriter =new FileWriter(file, true);
        	fileWriter.write("\r\n"+IpHost);
        	fileWriter.flush();
        	fileWriter.close();
        }
        
        Thread.currentThread().setContextClassLoader(HBaseConfiguration.class.getClassLoader());
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", IP);
        config.set("hbase.zookeeper.property.clientPort", "2181");        
        //config.set("hbase.master", IP);
        Connection con = ConnectionFactory.createConnection(config);//Connect to Database
        return con;

    }
    public static Connection ConnectHbase(String IP) throws Exception{
    	String[]  strs=IP.split("\\.");
    	String hostname="hdh"+strs[3];
    	return ConnectHbase(IP,hostname,"C:\\WINDOWS\\system32\\drivers\\etc\\hosts");
    }
    public static Connection ConnectHbase(String IP,String hostname) throws Exception{
    	String path="C:\\WINDOWS\\system32\\drivers\\etc\\hosts";
    	return ConnectHbase(IP,hostname,path);
    }
    
    
    /**
     * 关闭连接con.close()。
     * @param con 连接信息
     */
    public static void CloseCon(Connection con,String IP) throws Exception{
        con.close();
        InetAddress inetAddress=InetAddress.getByName(IP);
        String hostname = inetAddress.getHostName().toString();
        String IpHost = IP+" "+hostname;
        String s=null;
        ArrayList<String> all=new ArrayList<String>(); ;
        BufferedReader fileread =new BufferedReader(new FileReader("C:\\WINDOWS\\system32\\drivers\\etc\\hosts"));
        while((s = fileread.readLine()) != null) {
        	s=s.replaceAll(IpHost, "");
        		all.add(s);
        }
        fileread.close();
        BufferedWriter filewrite = new BufferedWriter(new FileWriter("C:\\WINDOWS\\system32\\drivers\\etc\\hosts"));
        for (int i = 0; i < all.size()-1; i++) {
        	if(all.get(i).length()!=0) {
        		filewrite.write(all.get(i));
        		filewrite.newLine();
        	}
        }
        filewrite.write(all.get(all.size()-1));
        filewrite.close();
    }
    
    /**
     * 建表，输入表名和列族名。
     * @param con 连接信息
     * @param tableName 表名
     * @param columnFamily 列族名
     */
    public static void CreateTable(Connection con, String tableName, String columnFamily) throws Exception{
        String[] columns = columnFamily.split(",");//get columns
        Admin admin=con.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "已存在");
        } else {
            
        	HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for(int i=0;i<columns.length;i++){
                tableDesc.addFamily(new HColumnDescriptor(columns[i]));
            }
            admin.createTable(tableDesc);
            System.out.println(tableName+"创建成功");
        }
        admin.close();
    }
    
    /**
     * 删除表，先disableTable，再deleteTable。
     * @param con 连接信息
     * @param tableName 表名
     */
    public static void DropTable(Connection con, String tableName) throws Exception{
    	Admin admin=con.getAdmin();
    	if (admin.tableExists(TableName.valueOf(tableName))) {
    		try  {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                System.out.println(tableName + "删除成功");
            }  catch  (Exception e) {
                e.printStackTrace();
            }
    	}
    }

    /**
     * 插入数据，需输入表名、rowkey、列族名、列名、值。
     * @param con 连接信息
     * @param tableName 表名
     * @param rowkey rowkey
     * @param family 列族
     * @param qualifier 列
     * @param Data 值
     */
    public static void InsertData(Connection con,String tableName,String rowkey,String family,String qualifier,String Data) throws Exception{
        //HTable table=new HTable(TableName.valueOf(tableName),con);
    	Table table=con.getTable(TableName.valueOf(tableName));
        Put put=new Put(Bytes.toBytes(rowkey));
        int i;
        i=Integer.parseInt(Data);
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier), Bytes.toBytes(i));//列族(family)，列(Qualifier),数据
        table.put(put);
        System.out.println(tableName + "插入成功" );
        table.close();
    }
    
    /**
     * 按rowkey查询所有值，并转为string返回,格式为{rowkey:XXX,Qualifier:XXX,...}。
     * @param con 连接信息
     * @param tableName 表名
     * @param rowkey rowkey
     */
    public static String SelectDataAll(Connection con,String tableName,String rowkey) throws Exception{
    	//HTable table=new HTable(TableName.valueOf(tableName),con);
    	Table table=con.getTable(TableName.valueOf(tableName));
    	Get get =  new  Get(Bytes. toBytes ( rowkey ));
    	Result r = table.get(get);
    	//System.out.println(r);
    	String result="{"+"rowkey:"+rowkey;
    	for  (Cell cell : r.rawCells()) {
    		//String a= new String(CellUtil. cloneValue (cell));
    		//System.out.println(a);
            result=result+","+Bytes. toString (CellUtil. cloneQualifier (cell))+":"+Bytes. toString (CellUtil. cloneValue (cell));
        }
    	result=result+"}";
    	System.out.println(result);
    	table.close();
        return result;
    }
    
    /**
     * 按rowkey查询特定列的值，并转为string返回。
     * @param con 连接信息
     * @param tableName 表名
     * @param rowkey rowkey
     * @param family 列族
     * @param qualifier 列
     */
    public static String SelectColumnData(Connection con,String tableName,String rowkey,String family,String qualifier) throws Exception{
    	//HTable table=new HTable(TableName.valueOf(tableName),con);
    	Table table=con.getTable(TableName.valueOf(tableName));
    	Get get =  new  Get(Bytes. toBytes ( rowkey ));
    	Result r = table.get(get);
    	byte[] idvalue = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    	System.out.println(Bytes.toString(idvalue));
    	table.close();
    	return Bytes.toString(idvalue);
    }
    
    /**
     * 返回表描述，describe TABLE。
     * @param con 连接信息
     * @param tableName 表名
     */
    public static String Describe(Connection con,String tableName) throws Exception {
    	Admin admin=con.getAdmin();
    	HTableDescriptor td = admin.getTableDescriptor(TableName.valueOf(tableName));
    	System.out.println(td);
    	return td.toString();
    }
    
    /**
     * 清空表，disable、drop、create三个动作的自动化集成。
     * @param con 连接信息
     * @param tableName 表名
     */
    public static void Truncate(Connection con,String tableName) throws Exception {
    	Admin admin=con.getAdmin();
    	HTableDescriptor td = admin.getTableDescriptor(TableName.valueOf(tableName));
    	if (admin.tableExists(TableName.valueOf(tableName))) {
    		try  {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                admin.createTable(td);
                System.out.println(tableName + "清空成功");
            }  catch  (Exception e) {
                e.printStackTrace();
            }
    	}
    }
    
    /**
     * 查询表行数，添加 FirstKeyOnlyFilter 过滤器的scan进行全表扫描，循环计数RowCount，速度较慢！
     * @param con 连接信息
     * @param tableName 表名
     */
    public static long rowCount(Connection con,String tableName) throws Exception{
    	long rowCount = 0;
    	        //HTable table=new HTable(name,con);
    	        Table table=con.getTable(TableName.valueOf(tableName));
    	        Scan scan = new Scan();
    	        //FirstKeyOnlyFilter只会取得每行数据的第一个kv，提高count速度
    	        scan.setFilter(new FirstKeyOnlyFilter());
    	        
    	        ResultScanner rs = table.getScanner(scan);
    	        for (Result result : rs) {
    	            rowCount += result.size();
    	        }
    	        System.out.println("RowCount: " + rowCount);
    	        table.close();
    	        return rowCount;
    }
    
    /**
     * 扫描全表，打印结果。
     * @param con 连接信息
     * @param tableName 表名
     */
    public static void ScanTable(Connection con,String tableName) throws Exception{
    	Scan scan = new Scan();
    	ResultScanner rs = null; 
    	//HTable table=new HTable(TableName.valueOf(tableName),con);
    	Table table=con.getTable(TableName.valueOf(tableName));
    	 rs = table.getScanner(scan);
    	 for (Result result = rs.next(); result != null; result = rs.next()) {
    		 System.out.println("Found row : " + result);
    	 }
    	 table.close();
    	 rs.close();
    	 
    }
    
    /**
     * 根据时间戳范围查数据，结束时间为当前时间
     * @param con 连接信息
     * @param tableName 表名
     * @param startTime 开始时间，String型，输入时间戳数值
     */
    public static ArrayList<String> TimeRangeAll(Connection con,String tableName,String startTime) throws Exception{
    	Long stopTime = System.currentTimeMillis();
    	return TimeRangeAll(con,tableName,startTime,String.valueOf(stopTime));
    }
    
    /**
     * 根据时间戳范围查数据，大于等于startTime，小于stopTime。
     * @param con 连接信息
     * @param tableName 表名
     * @param startTime 开始时间，String型，输入时间戳数值
     * @param stopTime 结束时间，String型，输入时间戳数值
     * @return 
     */
    public static ArrayList<String> TimeRangeAll(Connection con,String tableName,String startTime,String stopTime) throws Exception{
    	Scan scan = new Scan();
    	ResultScanner rs = null; 
    	ArrayList<String> AllResult = new ArrayList<String>();
    	//HTable table=new HTable(TableName.valueOf(tableName),con);
    	Table table=con.getTable(TableName.valueOf(tableName));
    	scan.setTimeRange(Long.valueOf(startTime), Long.valueOf(stopTime));
    	rs = table.getScanner(scan);
    	for (Result result = rs.next(); result != null; result = rs.next()) {
   		 System.out.println("Found row : " + result);
   		 AllResult.add(result.toString());
   	 }
    	table.close();
    	rs.close();
    	return AllResult;
    }
    
    /**
     * 根据时间戳范围查rowkey，结束时间为当前时间
     * @param con 连接信息
     * @param tableName 表名
     * @param startTime 开始时间，String型，输入时间戳数值
     */
    public static ArrayList<String> TimeRangeRowkey(Connection con,String tableName,String startTime) throws Exception{
    	Long stopTime = System.currentTimeMillis();
    	return TimeRangeRowkey(con,tableName,startTime,String.valueOf(stopTime));
    }
    
    /**
     * 根据时间戳范围查rowkey，大于等于startTime，小于stopTime。
     * @param con 连接信息
     * @param tableName 表名
     * @param startTime 开始时间，String型，输入时间戳数值
     * @param stopTime 结束时间，String型，输入时间戳数值
     * @return 
     */
    public static ArrayList<String> TimeRangeRowkey(Connection con,String tableName,String startTime,String stopTime) throws Exception{
    	Scan scan = new Scan();
    	ResultScanner rs = null; 
    	ArrayList<String> AllResult = new ArrayList<String>();
    	Table table=con.getTable(TableName.valueOf(tableName));
    	scan.setTimeRange(Long.valueOf(startTime), Long.valueOf(stopTime));
    	rs = table.getScanner(scan);
    	for (Result result : rs) {
    		    String rowkey = Bytes. toString (result.getRow());
    			System. out .println(rowkey);
    			AllResult.add(rowkey);
    	}
    	return AllResult;

    }

}

