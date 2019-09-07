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


public class KeggLoader {
	private static Logger logger = LoggerFactory.getLogger(KeggLoader.class);
	private String inFile;
	private OrientDBStore store = null;
	private String dbname;
	
	public KeggLoader(String infile, String sdbname, OrientDBStore sstore) {
		inFile = infile;
		store = sstore;
		dbname = sdbname;
	}

	/**
	 * dbname의 데이터베이스에 node, edge을 저장하기 위한 테이블을 생성한다. 
	 * @param dbname
	 * @return
	 */
	public int createTable() {
		/*
		String sql = "create class KeggNodes extends V";
		store.execute(dbname, sql);
		sql = "create class KeggEdges extends E";
		store.execute(dbname, sql);
		*/
		String sql = "delete Vertex KeggNodes";
		store.execute(dbname,  sql);
		
		sql = "delete Edge KeggEdges";
		store.execute(dbname,  sql);
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
				String[] ar_nodes = ar[0].split("\\|");
				String gene1 = ar_nodes[0].replace("'", "");
				String gene2 = ar_nodes[1].replace("'", "");
				// node  정보를 저장한다. 
				if(nodes.containsKey(gene1) == false) {
					nodes.put(gene1, sn ++);
				}
				if(nodes.containsKey(gene2) == false) {
					nodes.put(gene2, sn ++);
				}
				// edge 정보를 저장한다. 
				String edge1 = String.format("%s|%s",gene1, gene2 );
				if(links.containsKey(edge1) == false)  {
					ArrayList<String> list = new ArrayList<String>();
					list.add(gene1);
					list.add(gene2);
					//String result = Arrays.asList(ar).stream().map(Object::toString).reduce((t, u) -> t + "\t" + u).orElse("");
					//logger.info(result);
					if(ar.length < 2) {
						list.add("unknown");
					} else {
						String vv = ar[1].replace("'", "");
						list.add(vv);
					}
					links.put(edge1, list);
				}
				line = rp.readLine();
				co += 1;
			}
			saveNodes(nodes);
			saveLinks(links);
			logger.info(String.valueOf(nodes.size()));
			logger.info(String.valueOf(links.size()));
		
			for(String key : nodes.keySet()) {
				wp2.write(key + "\n");
			}
			
			for(String key : links.keySet()) {
				logger.info(key);
				wp.write(key + "\n");
				String result = Arrays.asList(links.get(key)).stream().map(Object::toString).reduce((t, u) -> t + "\t" + u).orElse("");
				logger.info(result);
			} 
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
			String sql = String.format("insert into KeggNodes set name='%s' ;", key);
			buf.append(sql);
			//logger.info(sql);
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
		StringBuilder buf = new StringBuilder();
		//buf.append("BEGINE;");
		int sn = 1;
		for(String key : hash.keySet()) {
			ArrayList<String> slist = hash.get(key);
			String sql = String.format("create edge KeggEdges from (select from KeggNodes where name='%s') to ( select from KeggNodes where name='%s') set link = '%s' ", 
					slist.get(0), slist.get(1), slist.get(2));
			this.store.execute(dbname, sql);
			if(sn % 100 == 0) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//buf.append(sql);
			
			/*
			if(sn % 500 == 0) {
				buf.append("COMMIT;");
				logger.info(buf.toString());
				this.store.execute(dbname, sql);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				buf = new StringBuilder();
				buf.append("BEGINE;");
			} */
			sn += 1;
		}
		if(buf.length() > 0) {
			buf.append("COMMIT;");
			this.store.execute(dbname, buf.toString());
		}
		
	}
}
