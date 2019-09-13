package com.fms.orientdb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BioGridLoader {
	
	private static Logger logger = LoggerFactory.getLogger(BioGridLoader.class);
	private String inFile;
	private OrientDBStore store = null;
	private String dbname;
	
	public BioGridLoader(String infile, String sdbname, OrientDBStore sstore) {
		this.inFile = infile;
		this.store = sstore;
		this.dbname = sdbname;
	}
	
	public int createTable() {
		String sql = "create class BioGridV extends V";
		store.execute(dbname, sql );
		sql = "create class BioGridE extends E"; 
		store.execute(dbname,  sql);
		sql = "delete Vertex BioGridV";
		store.execute(dbname , sql);
		sql = "delete Edge BioGridE";
		store.execute(dbname , sql);
		return 1;
	}
	
	public String load() {
		HashMap<String,Integer> nodes = new HashMap<String,Integer>();
		HashMap<String,ArrayList<String>> links = new HashMap<String,ArrayList<String>>();
		try {
			BufferedReader rp = new BufferedReader(new FileReader(new File(inFile)));
			BufferedWriter wp = new BufferedWriter(new FileWriter(new File("test.out")));
			BufferedWriter wp2 = new BufferedWriter(new FileWriter(new File("nodes.out")));
			String line = rp.readLine(); 
			int sn = 0;
			int link_sn = 0;
			int co = 0;
			while(line != null) {
				String[] ar = line.trim().split("\t");
				String gene1 = ar[0].replace("'", "");
				String gene2 = ar[1].replace("'", "");
				String link_p = ar[6];
				logger.info("link : " +  link_p);
				// node  정보를 저장한다. 
				if(nodes.containsKey(gene1) == false) {
					nodes.put(gene1, sn ++);
				}
				if(nodes.containsKey(gene2) == false) {
					nodes.put(gene2, sn ++);
				}
				// edge 정보를 저장한다. 
				String edge1 = String.format("%s|%s",gene1, gene2 );
				logger.info(edge1);
				if(links.containsKey(edge1) == false)  {
					ArrayList<String> list = new ArrayList<String>();
					list.add(gene1);
					list.add(gene2);
					list.add(link_p);
					//String result = Arrays.asList(ar).stream().map(Object::toString).reduce((t, u) -> t + "\t" + u).orElse("");
					//logger.info(result);
					links.put(edge1, list);
				}
				line = rp.readLine();
				co += 1;
			}
			saveNodes(nodes);
			saveLinks(links);
			logger.info(String.valueOf(nodes.size()));
			logger.info(String.valueOf(links.size()));
	
			/*
			for(String key : nodes.keySet()) {
				wp2.write(key + "\n");
			}
			
			for(String key : links.keySet()) {
				logger.info(key);
				wp.write(key + "\n");
				String result = Arrays.asList(links.get(key)).stream().map(Object::toString).reduce((t, u) -> t + "\t" + u).orElse("");
				logger.info(result);
			}  */
			rp.close();
			wp.close();
			wp2.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	private void saveNodes(HashMap<String, Integer> hash) {
		int sn = 1;
		StringBuilder buf = new StringBuilder();
		buf.append("BEGIN;");
		for(String key : hash.keySet()) {
			String sql = String.format("insert into BioGridV set name='%s' ;", key);
			buf.append(sql);
			if(sn % 500 == 0) {
				buf.append("COMMIT;");
				logger.info(buf.toString());
				this.store.execute(dbname, buf.toString());
				buf = new StringBuilder();
				buf.append("BEGIN;");
			}
			sn +=1;
		}
		
		if(buf.length() > 0) {
			buf.append("COMMIT;");
			this.store.execute(dbname, buf.toString());
		}
		logger.info(buf.toString());
	}
	
	private void saveLinks(HashMap<String, ArrayList<String>> hash) {
		int sn = 1;
		for(String key : hash.keySet()) {
			ArrayList<String> slist = hash.get(key);
			String sql = String.format("create edge BioGridE from (select from BioGridV where name='%s') to ( select from BioGridV where name='%s') set link = '%s' ", 
					slist.get(0), slist.get(1), slist.get(2));
			this.store.execute(dbname, sql);
			if(sn % 255 == 0) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			sn += 1;
		}
		
	}
	
	

}
