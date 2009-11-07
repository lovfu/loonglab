package com.kejikeji.lbs.service;

import java.util.List;

import com.kejikeji.lbs.model.Comment;
import com.kejikeji.lbs.model.Message;
import com.kejikeji.lbs.model.Rank;

public interface MessageService {
	/**
	 * ��ȡ������Ϣ�б�
	 * @param locationCode
	 * @return
	 */
	public List<Message> getLastedMessages(String locationCode);
	
	/**
	 * ��ȡָ����Ϣ
	 * @param id
	 * @return
	 */
	public Message getById(Long id);
	
	/**
	 * ����ĳ��Ϣ
	 * @param message
	 */
	public void publish(Message message);
	
	/**
	 * �������
	 * @param comment
	 */
	public void addComment(Comment comment);
	
	/**
	 * �������
	 * @param rank
	 */
	public void addRank(Rank rank);
}
