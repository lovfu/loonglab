package org.loonglab.hdc.imp.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.loonglab.hdc.common.HTablePoolFactory;

public class GroupService {
	public static List<Map<String, String>> groupDoc(PageBean page, String tenantId, 
			String type, Map<String, String> map_query, 
			String s_select, String group_by,String order_by,String notnull){
		ResultScanner scanner=null;
		HTableInterface table=null;
		try {
			Scan scan=new Scan(Bytes.toBytes("245_baidu_keywordROI_2012-02-09_"),Bytes.toBytes("245_baidu_keywordROI_2012-02-09_999999999"));
			scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("tenantId"));
//			
//			Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS, 
//				      new BinaryComparator(Bytes.toBytes("245_baidu_keywordROI_2012-02-09_999999999")));
//			Filter filter2 = new RowFilter(CompareFilter.CompareOp.GREATER, 
//				      new BinaryComparator(Bytes.toBytes("245_baidu_keywordROI_2012-02-09_")));
//			scan.setFilter(filter1);
//			scan.
			
			table=HTablePoolFactory.getPool().getTable("roi");
			scanner=table.getScanner(scan);
			
			for (Result result : scanner) {
				//System.out.println(new String(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("tenantId"))));
				System.out.println(result);
			}
			
			return null;
			
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(),e);
		} finally{
			if(scanner!=null)
				scanner.close();
			if(table!=null)
				try {
					table.close();
				} catch (IOException e) {
					//ignore
				}
		}
		
		
	}
	
	public static void main(String[] args) {
		groupDoc(null, null, null, null, null, null, null, null);
	}
}
