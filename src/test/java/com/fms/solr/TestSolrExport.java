package com.fms.solr;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSolrExport {
	
	private Logger logger = LoggerFactory.getLogger(TestSolrExport.class);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSolrDownloader() {
		logger.info(">>>>>> test solr downloader ");
		String[] args = new String[3];
		args[0] = "http://localhost:32768/solr/FmsGraph";
		args[1] = "*:*";
		args[2] = "./out/kegg_result.out";
		SolrDownloader dn = new SolrDownloader(args[0], args[1], args[2]);
		try {
			dn.download();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	//@Test
	public void testKeggExport() {
		logger.info(">>>>> test kegg solr export");
		String infile = "/home/moonstar/work2/kisti_work/dementia_network/storage_resource/KEGG/outfile/kegg.out";
		KeggSolrExporter ep = new KeggSolrExporter();
		ep.export(infile);
	}
	
	//@Test
	public void testKeggSearch() {
		String url = "http://localhost:32768/solr/FmsGraph";
		SolrStore solr = new SolrStore(url);
		HttpSolrClient ss = solr.open();
		SolrQuery qq = new SolrQuery();
		qq.set("q", "node1:MPI");
		qq.set("start", 0);
		qq.set("rows", 100);
		SolrDocumentList docList = solr.query(ss, qq);
		for(SolrDocument doc : docList) {
			FmsSolrBean bb = new FmsSolrBean();
			bb.setId((String) doc.getFieldValue("id"));
			bb.setNode1((String) doc.getFieldValue("node1"));
			bb.setNode2((String) doc.getFieldValue("node2"));
			List<String> ar = (List) doc.getFieldValues("property");
			bb.setProperty(ar.get(0));
			//bb.setProperty((String) doc.getFieldValues("property"));
			logger.info(bb.toString());
		}
		
		
	}

}
