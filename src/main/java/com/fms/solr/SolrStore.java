package com.fms.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

public class SolrStore {
	private String url;
	
	public SolrStore(String surl) {
		this.url = surl;
	}
	
	public HttpSolrClient open() {
		HttpSolrClient solr = new HttpSolrClient.Builder(this.url).build();
		return solr;
	}
	
	public void close(HttpSolrClient solr) {
		try {
			solr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SolrDocumentList query(HttpSolrClient solr, SolrQuery qq) {
		try {
			QueryResponse response = solr.query(qq);
			SolrDocumentList docList = response.getResults();
			return docList;
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	

}
