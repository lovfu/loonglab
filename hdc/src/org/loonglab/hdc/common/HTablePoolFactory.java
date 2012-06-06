package org.loonglab.hdc.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class HTablePoolFactory {
	private static HTablePool pool=null;
	
	public static synchronized HTablePool getPool(){
		if(pool!=null)
			return pool;
		
		Configuration conf=HBaseConfiguration.create();
		pool=new HTablePool(conf,10);
		
		return pool;
	}
}
