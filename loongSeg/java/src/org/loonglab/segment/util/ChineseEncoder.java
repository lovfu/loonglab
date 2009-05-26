package org.loonglab.segment.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.loonglab.segment.SegmentException;

public class ChineseEncoder {
	
	static Log log = LogFactory.getLog(ChineseEncoder.class);
	
	public static int hashGb2312(String s){
		try {
			byte[] ch=s.getBytes("gbk");
			
			//�����ǲ���gb2312����
			if ( ((ch[0] + 256) % 256 - 0xA0 < 16) || ((ch[0] + 256) % 256 - 0xA0 > 87) )//gb2312���ֱ����λ�ӵ�16������87��
			{
			   return -1;
			}
			if ( ((ch[1] + 256) % 256 - 0xA0 < 1) || ((ch[1] + 256) % 256 - 0xA0 > 94) )//gb2312���ֱ����λ��1��94
			{
			   return -1;
			}
			int sectionIndex = (ch[0] + 256) % 256 - 0xA0 - 16; //����(����Ϊ0),��16��Ϊgb2312ǰ15��û�ã��������Ŵӵ�16����ʼ
			int locationIndex = (ch[1] + 256) % 256 - 0xA0 - 1; //λ��(����Ϊ0),��1��Ϊgb2312λ�Ŵ�1��ʼ��ϣ����0��ʼ���ʼ�1

			int index = sectionIndex * 94 + locationIndex; //gb2312ÿ��94���ַ��������֤hash�Ľ������λ����һһ��Ӧ��

			return index;
		} catch (Exception e) {
			throw new SegmentException(e.getMessage(),e);
		}


	}
	
	public static int hashGbk(String s){
		
		try {
			byte[] ch=s.getBytes("gbk");
			
			int highIndex = (ch[0] + 256) % 256 - 0x81;
			int lowIndex;
			if ( (ch[1]+256)% 256 > 0x7f )
			{
			   lowIndex = (ch[1] + 256) % 256 - 0x40 - 1; //�ڶ��ֽڲ�����0x7f�����Եڶ��ֽڱ�0x7f����ٶ��1��������ֹhash�հ׿ռ���˷�
			}
			else
			{
			   lowIndex = (ch[1] + 256) % 256 - 0x40;
			}
			int index = highIndex * 190 + lowIndex;

			return index;
		} catch (Exception e) {
			
			log.error("s is "+s);
			throw new SegmentException(e.getMessage(),e);
		}
	}
	
	public static int hashCode(char c){
		//ascii���ַ�����
		if(c<256){
			return 6768+c;
		}
		//6768+256=7024������gbk�ַ���7024��ʼ
		StringBuffer s=new StringBuffer();
		s.append(c);
		int index=hashGb2312(s.toString());
		if(index==-1){
			//����GB2312�ܺ�����6768����ֹ�ظ�
			index=hashGbk(s.toString())+7024;
		}
		
		return index;
	}
	
	


}
