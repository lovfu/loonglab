package com.kejikeji.lbs.service;

import com.kejikeji.lbs.model.User;

public interface UserService {
	/**
	 * ע��
	 * @param user
	 */
	public void register(User user);
	
	/**
	 * ��¼����
	 * @param user
	 * @return
	 */
	public boolean login(User user);

}
