package com.arno.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class HbaseServiceTest {

	public static void main(String[] args){
		HbaseService.setHbaseZkQuorum("10.5.3.12,10.5.3.13", "2181");

		/**----统计和base 表行数性能测试------*/
		long start=System.nanoTime();
		System.out.println(HbaseService.getRowCount("callinfo"));
		long end=System.nanoTime();
		System.out.println(HbaseService.getRowCount("callinfo", "inf"));
		long end2=System.nanoTime();
		System.out.println(HbaseService.getRowCount(Bytes.toBytes("callinfo")));
		long end3=System.nanoTime();
		System.out.println(end-start);
		System.out.println(end2-end);
		System.out.println(end3-end2);
	}
}
