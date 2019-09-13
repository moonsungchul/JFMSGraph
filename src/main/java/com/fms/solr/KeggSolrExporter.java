package com.fms.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeggSolrExporter {
	private Logger logger = LoggerFactory.getLogger(KeggSolrExporter.class);
	private SolrStore solr = null;
	
	public KeggSolrExporter() {
		String url = "http://localhost:32768/solr/FmsGraph";
		this.solr = new SolrStore(url);
	}
	
	public String export(String fname) {
		try {
			HttpSolrClient ss = this.solr.open();
			BufferedReader rp = new BufferedReader(new FileReader(new File(fname)));
			String line = rp.readLine();
			ArrayList<FmsSolrBean> list = new ArrayList<FmsSolrBean>();
			while(line != null) {
				String[] ar = line.trim().split("\t");
				String[] ar_nodes = ar[0].split("\\|");
				String gene1 = ar_nodes[0].replace("'", "");
				String gene2 = ar_nodes[1].replace("'", "");
				//logger.info(line);
				String property = "";
				if(ar.length > 1) {
					property = ar[1].trim();
				}
				FmsSolrBean bean = new FmsSolrBean();
				bean.setNode1(gene1);
				bean.setNode2(gene2);
				bean.setProperty(property);
				//logger.info(bean.toString());
				list.add(bean);
				if(list.size() > 500) {
					logger.info(">>>>>>>>>>>>>>>>>>>>>>>>> list size  : ", list.size());
					ss.addBeans(list.iterator());
					ss.commit();
					list.clear();
				}
				//ss.addBean(bean);
				//ss.commit();
				line = rp.readLine();
			}
			if(list.size() > 0) {
				ss.addBeans(list.iterator());
				ss.commit();
			}
			rp.close();
			ss.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return "ok";
	}
	
	

}
