package com.arno.hbase;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseService {
	private static Configuration conf;
	private static HBaseAdmin admin=null;
	private static String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
	private static Log logger=LogFactory.getLog(HbaseService.class);
	
	/**
	 * 初始化配置
	 */
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1,127.0.0.2");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.client.retries.number", "1");
		conf.set("hbase.rootdir", "2181");
	}

	@Deprecated
	public static Configuration getConf() {
		return conf;
	}
	
	private static HBaseAdmin getAdmin(){
		try {
			if(admin==null){
				admin=new HBaseAdmin(conf);
			}
		} catch (MasterNotRunningException e) {
			logger.error(e);
		} catch (ZooKeeperConnectionException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		}
		return admin;
	}
	
	/**
	 * 配置zk地址和端口
	 * @param zks 多个地址，逗号分隔
	 * @param port
	 * @return
	 */
	public static void  setHbaseZkQuorum(String zks,String port){
		conf.set("hbase.zookeeper.quorum", zks);
		conf.set("hbase.zookeeper.property.clientPort", port);
	}
	
	/**
	 * 禁用table
	 * @param tableName 表名
	 */
	public static void disableTable(String tableName){
		try {
			getAdmin().disableTable(tableName);
		} catch (IOException e) {
			logger.error(e);
		}
	}
	
	/**
	 * 取消禁用table,启用table
	 * @param tableName 表名
	 */
	public static void enableTable(String tableName){
		try {
			getAdmin().enableTable(tableName);
		} catch (IOException e) {
			logger.error(e);
		}
	}
	
	/**
	 * 使用Scan与Filter的方式对表行数进行统计
	 * @param tableName 表名
	 * @return 行数
	 */
	public static long getRowCount(String tableName) {  
	    long rowCount = 0;  
	    try {  
	        HTable table = new HTable(conf, tableName);
	        
	        Scan scan = new Scan();  
	        scan.setFilter(new FirstKeyOnlyFilter());
	        ResultScanner resultScanner = table.getScanner(scan);  
	        for (Result result : resultScanner) {  
	            rowCount += result.size();  
	        }
	        table.close();
	    } catch (IOException e) {  
	        logger.error(e.getMessage(), e);  
	    }
	    return rowCount;  
	} 

	/**
	 * 对表行数进行统计
	 * @param byte[] tableName
	 * @return
	 */
	public static long getRowCount(byte[] tableName){
		return getRowCount(Bytes.toString(tableName),"");
	}
	
	public static long getRowCount(String tableName,String family){
		Scan scan = new Scan();
		if(StringUtils.isNotBlank(family)){
			scan.addFamily(Bytes.toBytes(family));
		}
		return getRowCount(tableName,scan);
	}
	
	public static long getRowCount(String startRow,String stopRow,String tableName,String familly){
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(stopRow));
		if(StringUtils.isNotBlank(familly)){
			scan.addFamily(Bytes.toBytes(familly));
		}
		return getRowCount(tableName,scan);
	}
	/**
	 * <pre>
	 * 使用Coprocessor新特性来对表行数进行统计 ,性能比直接用scan提高5倍
	 * 在Table注册了Coprocessor之后，在执行AggregationClient的时候，
	 * 会将RowCount分散到Table的每一个Region上，Region内RowCount的计算，是通过RPC执行调用接口，
	 * 由Region对应的RegionServer执行InternalScanner进行的。 
	 * 因此，性能的提升有两点原因:
	 * 1) 分布式统计。将原来客户端按照Rowkey的范围单点进行扫描，然后统计的方式，换成了由所有Region所在RegionServer同时计算的过程。
	 * 2）使用了在RegionServer内部执行使用了InternalScanner。这是距离实际存储最近的Scanner接口，存取更加快捷
	 * </pre>
	 * @param tableName 表名
	 * @param family 列族名称
	 * @return
	 */
	public static long getRowCount(String tableName, Scan scan) {
		HTableDescriptor htd = null;
		try {
			htd = getAdmin().getTableDescriptor(Bytes.toBytes(tableName));
		} catch (TableNotFoundException e1) {
			logger.error(e1);
		} catch (IOException e1) {
			logger.error(e1);
		}
		if (htd != null && !htd.hasCoprocessor(coprocessorClassName)) {
			addTableCoprocessor(tableName, coprocessorClassName, htd);
		}
		AggregationClient ac = new AggregationClient(conf);

		long rowCount = 0;
		try {
			HTable t = new HTable(conf, tableName);
			rowCount = ac.rowCount(t, new LongColumnInterpreter(), scan);
			//下面这三行，删除表的Copreocessor属性，必要时才需要；一般不需要。因为验证影响性能
//			if (htd != null && htd.hasCoprocessor(coprocessorClassName)) {
//				removeTableCoprocessor(tableName, coprocessorClassName, htd);
//			}
			
		} catch (IOException e) {
			logger.error(e);
		} catch (Throwable e) {
			logger.error(e);
		}finally{
			try {
				ac.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}

		return rowCount;
	}
	
	private static void addTableCoprocessor(String tableName, String coprocessorClassName,HTableDescriptor htd) {  
	    try {  
	        getAdmin().disableTable(tableName);  
	        htd.addCoprocessor(coprocessorClassName);  
	        getAdmin().modifyTable(Bytes.toBytes(tableName), htd);  
	        getAdmin().enableTable(tableName);  
	    } catch (IOException e) {  
	        logger.info(e.getMessage(), e);  
	    }  
	}  
	@SuppressWarnings("unused")
	private static void removeTableCoprocessor(String tableName, String coprocessorClassName,HTableDescriptor htd) {  
	    try {  
	        getAdmin().disableTable(tableName);  
	        htd.removeCoprocessor(coprocessorClassName);
	        getAdmin().modifyTable(Bytes.toBytes(tableName), htd);  
	        getAdmin().enableTable(tableName);  
	    } catch (IOException e) {  
	        logger.info(e.getMessage(), e);  
	    }  
	}  
}
