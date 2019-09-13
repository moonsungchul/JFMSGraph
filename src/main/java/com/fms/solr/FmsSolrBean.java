package com.fms.solr;

import org.apache.solr.client.solrj.beans.Field;

public class FmsSolrBean {
	String id;
	String node1;
	String node2;
	String property;
	
	public FmsSolrBean() {
		
	}
	
	public FmsSolrBean(String id, String node1, String node2, String property) {
		super();
		this.id = id;
		this.node1 = node1;
		this.node2 = node2;
		this.property = property;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getNode1() {
		return node1;
	}

	@Field("node1")
	public void setNode1(String node1) {
		this.node1 = node1;
	}

	public String getNode2() {
		return node2;
	}

	@Field("node2")
	public void setNode2(String node2) {
		this.node2 = node2;
	}

	public String getProperty() {
		return property;
	}

	@Field("property")
	public void setProperty(String property) {
		this.property = property;
	}

	@Override
	public String toString() {
		return "FmsSolrBean [id=" + id + ", node1=" + node1 + ", node2=" + node2 + ", property=" + property + "]";
	}
	
	
	


	
	
	
	
	

}
