package com.fms.solr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrDownloader {
	private Logger logger = LoggerFactory.getLogger(SolrDownloader.class);
	private String url;
	private String outpath;
	private String query;
	private SolrStore solr =null;
	
	public SolrDownloader(String url, String query, String outpath) {
		this.url = url;
		this.query = query;
		this.outpath = outpath;
		this.solr = new SolrStore(this.url);
	}
	
	public void download() throws IOException {
		BufferedWriter wp = new BufferedWriter(new FileWriter(new File(this.outpath)));
		SolrQuery qq = new SolrQuery();
		qq.set("q", this.query);
		qq.set("start", 0);
		qq.set("rows", 1);
		
		this.solr.open();
		HttpSolrClient con = this.solr.open();
		try {
			SolrDocumentList docs = this.solr.query(con, qq);
			long total = docs.getNumFound();
			for(int i=0;i<total;i+=10000) {
				qq.set("start", i);
				qq.set("rows",  10000);
				docs = this.solr.query(con, qq);
				this.saveResult(wp, docs);
			}
			con.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void saveResult(BufferedWriter wp, SolrDocumentList docs) {
		for(SolrDocument doc : docs) {
			FmsSolrBean bb = new FmsSolrBean();
			bb.setId((String) doc.getFieldValue("id"));
			bb.setNode1((String) doc.getFieldValue("node1"));
			bb.setNode2((String) doc.getFieldValue("node2"));
			List<String> ar = (List) doc.getFieldValues("property");
			if(ar != null && ar.size() > 0) {
				bb.setProperty(ar.get(0));
			} else {
				bb.setProperty("");
			} 
			try {
				wp.write(bb.toFileString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public static void main(String[] args) {
		for(int i=0;i<args.length;i++) {
			System.out.println(args[i]);
		}
		if(args.length !=3) {
			System.out.println("SolrDownloader <solr url> <solr query> <outpath>");
			System.exit(1);
		}
		
		SolrDownloader dn = new SolrDownloader(args[0], args[1], args[2]);
		try {
			dn.download();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	

}
