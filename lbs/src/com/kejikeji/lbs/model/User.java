package com.kejikeji.lbs.model;

/**
 * @hibernate.class table="u_user" dynamic-update="true"
 * @author xpdragon
 *
 */
public class User {
	/**
	 * @hibernate.id generator-class="native"
	 */
	private Long id;
	/**
	 * @hibernate.property
	 */
	private String name;
	/**
	 * @hibernate.property column="passwd"
	 */
	private String password;
	/**
	 * @hibernate.many-to-one class="com.kejikeji.lbs.model.LocationCatalog"
	 */
	private LocationCatalog curLocation;
	
	
	
	
	public User(Long id) {
		super();
		this.id = id;
	}
	public User() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String passwd) {
		this.password = passwd;
	}
	public LocationCatalog getCurLocation() {
		return curLocation;
	}
	public void setCurLocation(LocationCatalog curLocation) {
		this.curLocation = curLocation;
	}

	
	
	
}