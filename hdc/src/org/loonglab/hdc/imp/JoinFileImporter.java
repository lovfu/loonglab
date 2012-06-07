package org.loonglab.hdc.imp;

import java.io.File;
import java.io.FilenameFilter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.loonglab.hdc.common.ApplicationProperties;


public class JoinFileImporter {
	
	private static Log log = LogFactory.getLog(JoinFileImporter.class);
	//Map<String, CommonIndexer> indexerMap=new HashMap<String, CommonIndexer>();
	
	public static int THREAD_COUNT = 2;
	ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
	
	//按天数分别索引，防止内存溢出
	public void importByDays(String searchEngine,String roiType,String startDate,String endDate,String tenantId,int dayNum){
		if(startDate==null||endDate==null
				||"".equals(startDate)||"".equals(endDate)
				||"-".equals(startDate)||"-".equals(endDate)){
			log.warn("startDate and endDate can't be empty!!!");
			return;
		}
		
		String dateQuery="["+startDate+" TO "+endDate+"]";
		List<String> dayList=getAllDates(dateQuery);
		
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
	        	importJoinFile(searchEngine, roiType, sdate, edate, tenantId);
	        	
	        	sdate=null;
	        	edate=null;
	        	curNum=0;
	        }
	        
        }
		
		if(sdate!=null&&edate==null){
			edate=dayList.get(dayList.size()-1);
			//log.info(sdate+"~"+edate);
			importJoinFile(searchEngine, roiType, sdate, edate, tenantId);
		}
	}
	
	public void importJoinFile(String searchEngine,String roiType,String startDate,String endDate,String tenantId){	
		
		log.info("import join file from "+startDate+" to "+endDate+"("+searchEngine+","+roiType+","+tenantId+")...");
		//List<String> results=new ArrayList<MonitorResult>();
		try {
	        int n=0;
	        CompletionService<String> ecs = new ExecutorCompletionService<String>(pool);
	        
	        long startTime=System.currentTimeMillis();
	        String rootDir=ApplicationProperties.getProperties("join.file.dir");
	        File root=new File(rootDir);
	        
	        File[] seDirs=root.listFiles(new CommaDelimitFilenameFilter(searchEngine));
	        
	        for (File seDir : seDirs) {
	        	File[] roiTypeDirs=seDir.listFiles(new CommaDelimitFilenameFilter(roiType));
	        	for (File roiTypeDir : roiTypeDirs) {
	        		File[] dateDirs=roiTypeDir.listFiles(new DateFilenameFilter(startDate, endDate));
	        		for (File dateDir : dateDirs) {
	        			File[] tenantFiles=dateDir.listFiles(new CommaDelimitFilenameFilter(tenantId));
	        			for (File joinFile : tenantFiles) {							
	        				//indexFile(joinFile);
	        				ImpFileHandler handler=new ImpFileHandler(joinFile);
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
	
	private List<String> getAllDates(String dateQuery){
		
		try {
			List<String> list=new ArrayList<String>();
			
			String[] temp=dateQuery.split(" ");
			String startDate=temp[0].substring(1);
			String endDate=temp[2].substring(0, temp[2].length()-1);
			
			Date sDate=DateUtils.parseDate(startDate, new String[]{"yyyy-MM-dd"});			
			
			
			Date eDate=DateUtils.parseDate(endDate, new String[]{"yyyy-MM-dd"});
			while(!DateUtils.isSameDay(sDate, eDate)){
				list.add(DateFormatUtils.format(sDate, "yyyy-MM-dd"));
				sDate=DateUtils.addDays(sDate, 1);
			}
			
			list.add(endDate);
			
			return list;
		} catch (ParseException e) {
			log.error(e.getMessage(),e);
			throw new RuntimeException(e.getMessage(),e);
		}
	}
	
	public void shutdown(){
		pool.shutdown();
	}
	
	public static void main(String[] args) {
		JoinFileImporter importer=new JoinFileImporter();
		importer.importJoinFile("baidu", "-", "2012-02-09", "2012-02-09", "245");
		importer.shutdown();
	}
}
