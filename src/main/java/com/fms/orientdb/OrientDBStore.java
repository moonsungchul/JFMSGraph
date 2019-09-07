package com.fms.orientdb;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class OrientDBStore {
	
	private String url;
	private String user;
	private String passwd;
	
	
	public OrientDBStore(String surl, String suser, String spasswd) {
		url = surl;
		user = suser;
		passwd = spasswd;
	}
	
	public OrientDB open() {
		OrientDB store = new OrientDB(url, user, passwd, OrientDBConfig.defaultConfig());
		return store;
	}

	/**
	 * 새로운 데이터베이스를 생성한다. 
	 * @param dbname
	 */
	public void createDatabase(String dbname) {
		OrientDB store = open();
		store.create(dbname, ODatabaseType.PLOCAL );
		store.close();
	}

	/**
	 * batch query을 실행한다. 
	 * @param dbname
	 * @param query
	 */
	public void batch(String dbname, String query) {
		OrientDB store = open();
		ODatabasePool pool = new ODatabasePool(store, dbname, user, passwd);
		try(ODatabaseSession db = pool.acquire()) {
			db.execute("sql", query);
		} finally {
			pool.close();
			store.close();
		}
	}
	
	public void execute(String dbname, String query) {
		OrientDB store = open();
		ODatabasePool pool = new ODatabasePool(store, dbname, user, passwd);
		try(ODatabaseSession db = pool.acquire()) {
			db.execute("sql", query);
		} finally {
			pool.close();
			store.close();
		}
	}
	
	public OResultSet select(String dbname, String query) {
		OrientDB store = open();
		ODatabasePool pool = new ODatabasePool(store, dbname, user, passwd);
		OResultSet rec = null;
		try(ODatabaseSession db = pool.acquire()) {
			rec = db.query(query);
		} finally {
			pool.close();
			store.close();
		}
		return rec;
	}
	
	
	
	
	
	
	
	
	
	

}
