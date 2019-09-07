package com.fms.orientdb;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.db.OrientDB;

public class TestOrientDB {
	
	private static Logger logger = LoggerFactory.getLogger(TestOrientDB.class);

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

	//@Test
	public void test() {
		logger.info(">>>>>>>>>>>>>>> test ");
		String url = "remote:localhost";
		String user = "moonstar";
		String passwd = "wooag01";
		OrientDBStore store = new OrientDBStore(url, user, passwd);
		//store.createDatabase("FmsGraph");
		String sql = "create class Node";
		store.execute("FmsGraph", sql);
		
	}
	
	//@Test 
	public void testKeggLoader() {
		logger.info("Test Kegg Data");
		String url = "remote:localhost";
		String user = "moonstar";
		String passwd = "wooag01";
		OrientDBStore store = new OrientDBStore(url, user, passwd);
		String infile = "/home/moonstar/work2/kisti_work/dementia_network/storage_resource/KEGG/outfile/kegg.out";
		KeggLoader ko = new KeggLoader(infile, "FmsGraph", store);
		ko.createTable();
		ko.load();
	}
	
	@Test 
	public void testBioGridLoader() {
		logger.info("Test BioGrid Data");
		String url = "remote:localhost";
		String user = "moonstar";
		String passwd = "wooag01";
		OrientDBStore store = new OrientDBStore(url, user, passwd);
		String infile = "/home/moonstar/work2/kisti_work/dementia_network/storage_resource/BIOGRID/2016_08_09/master_BIOGRID-ALL-3.4.136.mitab.txt";
		BioGridLoader ko = new BioGridLoader(infile, "FmsGraph", store);
		ko.createTable();
		ko.load();
	}
	

}
