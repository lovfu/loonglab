package org.ictclas4j.segment;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ictclas4j.bean.Atom;
import org.ictclas4j.bean.MidResult;
import org.ictclas4j.bean.SegNode;
import org.ictclas4j.bean.SegResult;
import org.ictclas4j.bean.Sentence;
import org.ictclas4j.bean.TxtDictionary;
import org.ictclas4j.utility.POSTag;
import org.ictclas4j.utility.Utility;

public class SegTag {
	
	static Log log = LogFactory.getLog(SegTag.class);
	
	private TxtDictionary coreDict;
	private TxtDictionary bigramDict;
	private PosTagger personTagger;
	private PosTagger transPersonTagger;
	private PosTagger placeTagger;
	private PosTagger lexTagger;

	private int segPathCount = 1;// �ִ�·������Ŀ��N-���·���е�N��

	public SegTag(int segPathCount) {
		this.segPathCount = segPathCount;
		initDictionary("dic");
	}
	
	public SegTag(String dicDir){
		initDictionary(dicDir);
	}
	
	public void initDictionary(String dicDir){
		coreDict = new TxtDictionary(dicDir+"/coreDict.dct");
		coreDict.loadBiDict(dicDir+"/bigramDict.dct");
		//bigramDict = new TxtDictionary(dicDir+"/bigramDict.dct");
		coreDict.loadUserDict(dicDir+"/userDict.dct");
		
		personTagger = new TxtPosTagger(Utility.TAG_TYPE.TT_PERSON, dicDir+"/nr", coreDict);
		transPersonTagger = new TxtPosTagger(Utility.TAG_TYPE.TT_TRANS_PERSON, dicDir+"/tr", coreDict);
		placeTagger = new TxtPosTagger(Utility.TAG_TYPE.TT_PLACE, dicDir+"/ns", coreDict);
		lexTagger = new TxtPosTagger(Utility.TAG_TYPE.TT_NORMAL, dicDir+"/lexical", coreDict);
	}
	
	

	public SegResult split(String src) {
		
		long allStart=System.currentTimeMillis();
		
		SegResult sr = new SegResult(src);// �ִʽ��
		String finalResult = null;

		if (src != null) {
			finalResult = "";
			int index = 0;
			String midResult = null;
			sr.setRawContent(src);
			SentenceSeg ss = new SentenceSeg(src);
			ArrayList<Sentence> sens = ss.getSens();
			
			long allSenCost=0;
			
			long senCost1=0;
			long senCost2=0;
			long graphCost=0;
			long graphCost1=0;
			
			for (Sentence sen : sens) {
				
				long senStart=System.currentTimeMillis();
				//log.debug("process "+sen.getContent()+"...");
				
				long start=System.currentTimeMillis();
				MidResult mr = new MidResult();
				mr.setIndex(index++);
				mr.setSource(sen.getContent());
				
				long senMid=System.currentTimeMillis();
				if (sen.isSeg()) {
					// ԭ�ӷִ�
					AtomSeg as = new AtomSeg(sen.getContent());
					ArrayList<Atom> atoms = as.getAtoms();
					mr.setAtoms(atoms); 
					println2Err("[atom time]:"+(System.currentTimeMillis()-start));
					start=System.currentTimeMillis();
					
					
					
					// ���ɷִ�ͼ��,�Ƚ��г����ִʣ�Ȼ������Ż��������д��Ա��
					SegGraph segGraph = GraphGenerate.generate(atoms, coreDict);
					mr.setSegGraph(segGraph.getSnList());
					
//					log.debug("===segGraph===");
//					int ii=0;
//					for (SegNode node : segGraph.getSnList()) {
//						log.debug((ii++)+","+node);
//					}
					
					graphCost1+=System.currentTimeMillis()-start;
					
					// ���ɶ���ִ�ͼ��
					SegGraph biSegGraph = GraphGenerate.biGenerate(segGraph, coreDict, bigramDict);
					mr.setBiSegGraph(biSegGraph.getSnList());
					println2Err("[graph time]:"+(System.currentTimeMillis()-start));
					
					graphCost+=System.currentTimeMillis()-start;
					
					start=System.currentTimeMillis();
					
					
					
					// ��N���·��
					NShortPath nsp = new NShortPath(biSegGraph, segPathCount);
					ArrayList<ArrayList<Integer>> bipath = nsp.getPaths();
					mr.setBipath(bipath);
					println2Err("[NSP time]:"+(System.currentTimeMillis()-start));
					start=System.currentTimeMillis();
					
//					log.debug("===nsp===");
//					for (ArrayList<Integer> arrayList : bipath) {
//						for (Integer integer : arrayList) {
//							log.debug(segGraph.getSnList().get(integer));
//						}
//						log.debug("=====");
//					}
					
					senCost1 = senCost1+(System.currentTimeMillis()-senStart);
					
					senMid=System.currentTimeMillis();
					
					for (ArrayList<Integer> onePath : bipath) {
						// �õ����ηִ�·��
						ArrayList<SegNode> segPath = getSegPath(segGraph, onePath);
						ArrayList<SegNode> firstPath = AdjustSeg.firstAdjust(segPath);
						String firstResult = outputResult(firstPath);
						mr.addFirstResult(firstResult);
						println2Err("[first time]:"+(System.currentTimeMillis()-start));
						start=System.currentTimeMillis();

						// ����δ��½�ʣ����Գ��ηִʽ�������Ż�
						SegGraph optSegGraph = new SegGraph(firstPath);
						ArrayList<SegNode> sns = clone(firstPath);
						personTagger.recognition(optSegGraph, sns);
						transPersonTagger.recognition(optSegGraph, sns);
						placeTagger.recognition(optSegGraph, sns);
						mr.setOptSegGraph(optSegGraph.getSnList());
						println2Err("[unknown time]:"+(System.currentTimeMillis()-start));
						start=System.currentTimeMillis();

						// �����Ż���Ľ�������½������ɶ���ִ�ͼ��
						SegGraph optBiSegGraph = GraphGenerate.biGenerate(optSegGraph, coreDict, bigramDict);
						mr.setOptBiSegGraph(optBiSegGraph.getSnList());

						// ������ȡN�����·��
						NShortPath optNsp = new NShortPath(optBiSegGraph, segPathCount);
						ArrayList<ArrayList<Integer>> optBipath = optNsp.getPaths();
						mr.setOptBipath(optBipath);

						// �����Ż���ķִʽ�������Խ�����д��Ա�Ǻ������Ż���������
						ArrayList<SegNode> adjResult = null;
						for (ArrayList<Integer> optOnePath : optBipath) {
							ArrayList<SegNode> optSegPath = getSegPath(optSegGraph, optOnePath);
							lexTagger.recognition(optSegPath);
							String optResult = outputResult(optSegPath);
							mr.addOptResult(optResult);
							adjResult = AdjustSeg.finaAdjust(optSegPath, personTagger, placeTagger);
							String adjrs = outputResult(adjResult);
							println2Err("[last time]:"+(System.currentTimeMillis()-start));
							start=System.currentTimeMillis();
							if (midResult == null)
								midResult = adjrs;
							break;
						}
					}
					sr.addMidResult(mr);
				} else
					midResult = sen.getContent();
				finalResult += midResult;
				midResult = null;
				
				
				long senCost=System.currentTimeMillis()-senStart;
				//log.debug("sentence split cost "+(senCost)+"ms");
				
				allSenCost+=senCost;
				
				senCost2=senCost2+(System.currentTimeMillis()-senMid);
				
				
			}

			log.debug("allSenCost is "+allSenCost);
			log.debug("senCost1="+senCost1+",senCost2="+senCost2+",graphcost="+graphCost+"," +
					"graphCost1="+graphCost1+",findWordCost="+GraphGenerate.findWordCost+",getHandleCost="+GraphGenerate.getHandlecost);
			
			sr.setFinalResult(finalResult);
		}
		
		log.debug("split cost "+(System.currentTimeMillis()-allStart)+"ms");

		return sr;
	}

	private ArrayList<SegNode> clone(ArrayList<SegNode> sns) {
		ArrayList<SegNode> result = null;
		if (sns != null && sns.size() > 0) {
			result = new ArrayList<SegNode>();
			for (SegNode sn : sns)
				result.add(sn.clone());
		}
		return result;
	}

	// ���ݶ���ִ�·�����ɷִ�·��
	private ArrayList<SegNode> getSegPath(SegGraph sg, ArrayList<Integer> bipath) {
		ArrayList<SegNode> path = null;

		if (sg != null && bipath != null) {
			ArrayList<SegNode> sns = sg.getSnList();
			path = new ArrayList<SegNode>();

			for (int index : bipath)
				path.add(sns.get(index));
		}
		return path;
	}

	// ���ݷִ�·�����ɷִʽ��
	private String outputResult(ArrayList<SegNode> wrList) {
		String result = null;
		String temp=null;
		char[] pos = new char[2];
		if (wrList != null && wrList.size() > 0) {
			result = "";
			for (int i = 0; i < wrList.size(); i++) {
				SegNode sn = wrList.get(i);
				if (sn.getPos() != POSTag.SEN_BEGIN && sn.getPos() != POSTag.SEN_END) {
					int tag = Math.abs(sn.getPos());
					pos[0] = (char) (tag / 256);
					pos[1] = (char) (tag % 256);
					temp=""+pos[0];
					if(pos[1]>0)
						temp+=""+pos[1];
					result += sn.getSrcWord() + "/" + temp + " ";
				}
			}
		}

		return result;
	}

	public void setSegPathCount(int segPathCount) {
		this.segPathCount = segPathCount;
	}
	
	public static void main(String[] args) {
		SegTag segTag = new SegTag(1);	
		
		segTag.outputDic();
		
//		BufferedReader reader= new BufferedReader(new InputStreamReader(System.in));
//		String line=null;
//		try {
//			while ((line=reader.readLine())!=null) {
//				try { 
//					SegResult seg_res=segTag.split(line);
//					System.out.println(seg_res.getFinalResult());
//				} catch (Throwable t) {
//					t.printStackTrace();					
//				}
//			}
//		} catch (IOException e) {			
//			e.printStackTrace();
//		}						
	}
	
	public void outputDic(){
		coreDict.outputChars("dic/coreDict.dct");
		bigramDict.outputChars("dic/bigramDict.dct");
		personTagger.getUnknownDict().outputChars("dic/nr.dct");
		transPersonTagger.getUnknownDict().outputChars("dic/tr.dct");
		placeTagger.getUnknownDict().outputChars("dic/ns.dct");
		personTagger.getContextStat().outputChars("dic/nr.ctx");
		transPersonTagger.getContextStat().outputChars("dic/tr.ctx");
		placeTagger.getContextStat().outputChars("dic/ns.ctx");
		lexTagger.getContextStat().outputChars("dic/lexical.ctx");
	}
	
	private static void println2Err(String str) {
		//System.err.println(str);		
	}
}
