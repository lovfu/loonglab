package org.loonglab.hdc.imp;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cubead.datacenter.ConfigUtil;
import com.cubead.datacenter.Constants;
import com.cubead.datacenter.index.parser.CsvFileParser;
import com.cubead.datacenter.search.GroupUtil;


public class JoinFileIndexer {
	
	private static Log log = LogFactory.getLog(JoinFileIndexer.class);
	//Map<String, CommonIndexer> indexerMap=new HashMap<String, CommonIndexer>();
	
	public static int THREAD_COUNT = 2;
	ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
	
	//按天数分别索引，防止内存溢出
	public void reIndexByDays(String searchEngine,String roiType,String startDate,String endDate,String tenantId,int dayNum){
		if(startDate==null||endDate==null
				||"".equals(startDate)||"".equals(endDate)
				||"-".equals(startDate)||"-".equals(endDate)){
			log.warn("startDate and endDate can't be empty!!!");
			return;
		}
		
		String dateQuery="["+startDate+" TO "+endDate+"]";
		List<String> dayList=GroupUtil.getAllDates(dateQuery);
		
		String sdate=null;
		String edate=null;
		int curNum=0;
		for (int i = 0; i < dayList.size(); i++) {
			if(curNum==0){
				sdate=dayList.get(i);
			}
			curNum++;
			if(curNum==dayNum){
	        	edate=dayList.get(i);
	        	//log.info(sdate+"~"+edate);
	        	reIndex(searchEngine, roiType, sdate, edate, tenantId);
	        	
	        	sdate=null;
	        	edate=null;
	        	curNum=0;
	        }
	        
	        
//	        if(sdate!=null&&edate!=null){	        	
//	        	log.info(sdate+"~"+edate);
//	        	//reIndex(searchEngine, roiType, sdate, edate, tenantId);
//	        	sdate=null;
//	        	edate=null;
//	        }
        }
		
		if(sdate!=null&&edate==null){
			edate=dayList.get(dayList.size()-1);
			//log.info(sdate+"~"+edate);
			reIndex(searchEngine, roiType, sdate, edate, tenantId);
		}
	}
	
	public void reIndex(String searchEngine,String roiType,String startDate,String endDate,String tenantId){	
		
		log.info("reIndex join file from "+startDate+" to "+endDate+"("+searchEngine+","+roiType+","+tenantId+")...");
		//List<String> results=new ArrayList<MonitorResult>();
		try {
	        int n=0;
	        CompletionService<String> ecs = new ExecutorCompletionService<String>(pool);
	        
	        long startTime=System.currentTimeMillis();
	        File root=new File(ConfigUtil.getJoinRoot());
	        
	        File[] seDirs=root.listFiles(new CommaDelimitFilenameFilter(searchEngine));
	        
	        for (File seDir : seDirs) {
	        	File[] roiTypeDirs=seDir.listFiles(new CommaDelimitFilenameFilter(roiType));
	        	for (File roiTypeDir : roiTypeDirs) {
	        		File[] dateDirs=roiTypeDir.listFiles(new DateFilenameFilter(startDate, endDate));
	        		for (File dateDir : dateDirs) {
	        			File[] tenantFiles=dateDir.listFiles(new CommaDelimitFilenameFilter(tenantId));
	        			for (File joinFile : tenantFiles) {							
	        				//indexFile(joinFile);
	        				IndexFileHandler handler=new IndexFileHandler(joinFile);
	        				ecs.submit(handler);
	        				n++;
	        			}
	        		}
	        	}
	        }
	        
	        for (int i = 0; i < n; i++) {
	            String fileName=ecs.take().get();
	            log.info("("+(i+1)+"/"+n+")index "+fileName+" finish!");
	        }
	        
	        log.info("join file reindex cost "+(System.currentTimeMillis()-startTime)+"ms");
        }
        catch (Exception e) {
	        throw new RuntimeException(e.getMessage(),e);
        }
	}
	
	
	
	
	
	class IndexFileHandler implements Callable<String> {
		
		File joinFile;
		
		public IndexFileHandler(File joinFile) {
	        super();
	        this.joinFile = joinFile;
        }



		@Override
        public String call() throws Exception {
			log.debug("start index "+joinFile.getCanonicalPath()+"...");
	        indexFile(joinFile);
	        return joinFile.getCanonicalPath();
        }
		
		private void indexFile(File joinFile){
			CsvFileParser parser=null;
			try {
				String tenantId=joinFile.getName().substring(0, joinFile.getName().length()-4);
				String type=joinFile.getParentFile().getParentFile().getName();
				String indexType=Constants.ROI_TYPE_INDEX_MAP.get(Constants.FILE_TYPE_MAP.get("/"+type));
				String statDate=joinFile.getParentFile().getName();
				String searchEngine=joinFile.getParentFile().getParentFile().getParentFile().getName();
				//log.info("index "+tenantId+","+indexType);		
//				CommonIndexer indexer=indexerMap.get(tenantId+"/"+indexType);
//				if(indexer==null){
//					indexer=new CommonIndexer(tenantId, indexType);
//					indexerMap.put(tenantId+"/"+indexType, indexer);
//				}
				
				CommonIndexer indexer=new CommonIndexer(tenantId, indexType);
				
				String deleteQuery="searchEngine:"+searchEngine+" AND statDate:"+statDate;
				indexer.delete(deleteQuery);
			
				//log.info("index "+joinFile.getCanonicalPath()+"...");
				//indexer=new CommonIndexer(tenantId, indexType);
		        parser=new CsvFileParser(joinFile);
		        while(parser.hasNext()){
		        	Map<String, String> row=parser.next();
		        	indexer.addDocument(row);
		        	//indexer.updateDocument(row);
		        }
		        
		        indexer.close();
	        }
	        finally {
		        if(parser!=null)
		        	parser.close();
	        }
		}
		
	}
	
	
	class DateFilenameFilter implements FilenameFilter{
		private String sDate;
		private String eDate;
		
		public DateFilenameFilter(String sDate, String eDate) {
			super();
			this.sDate = sDate;
			this.eDate = eDate;
		}

		@Override
		public boolean accept(File dir, String name) {
			if("-".equals(sDate)&&"-".equals(eDate))
				return true;
			
			if(name.compareTo(sDate)>=0&&name.compareTo(eDate)<=0)
				return true;
			
			
			return false;
		}
		
	}
	
	class CommaDelimitFilenameFilter implements FilenameFilter{

		private String[] filenames;
		
		public CommaDelimitFilenameFilter(String commaDelimitName) {
			super();
			filenames=commaDelimitName.split(",");
		}

		@Override
		public boolean accept(File dir, String name) {
			if(filenames.length==1&&(filenames[0].equals("-")||filenames[0].equals("")))
				return true;
			
			for (String filename : filenames) {
				if(name.contains(filename))
					return true;
			}

			return false;
		}
		
	}
}
