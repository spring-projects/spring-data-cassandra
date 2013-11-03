/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.extern.log4j.Log4j;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.bean.RingMember;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * CQL3 Protocol implementation of {@link CassandraOperations} using the DataStax Java Driver Client.
 * 
 * @author David Webb
 */
@Log4j
public class CassandraTemplate implements CassandraOperations {

	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();
	private Session session;
	
	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param cassandraFactory
	 */
	public CassandraTemplate(Session session) {
		this.session = session;
	}

	/**
	 * Return the keyspace client
	 * @return
	 */
	public Session getSession() {
		return session;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeRing()
	 */
	public List<RingMember> describeRing() {
	
		/*
		 * Return variable
		 */
		List<RingMember> ring = new ArrayList<RingMember>();

		/*
		 * Get list of token ranges from the Session
		 */
		Metadata meta = session.getCluster().getMetadata();
		
		/*
		 * Convert them to generic beans for future implementations.
		 */
		RingMember member = null;
		for (Host host: meta.getAllHosts()) {
			
			member = new RingMember();
			member.setAddress(host.getAddress().getHostAddress());
			member.setDC(host.getDatacenter());
			member.setRack(host.getRack());
			
			ring.add(member);
		}
		
		/*
		 * Return
		 */
		return ring;
		
	}


	/**
	 * Execute a command at the ColumnFamily Level
	 * 
	 * @param cfName
	 * @param callback
	 * @return
	 */
//	protected <T> T execute(String cfName, ColumnFamilyCallback<T> callback) {
//
//		Assert.notNull(cfName);
//		Assert.notNull(callback);
//
//		try {
//			ColumnFamily<String, String> CF =
//					  new ColumnFamily<String, String>(
//							  cfName,              
//							  StringSerializer.get(), 
//							  StringSerializer.get());
//			
//			return callback.doInColumnFamily(CF);
//			
//		} catch (Exception e) {
//			throw potentiallyConvertRuntimeException(e);
//		}
//	}
	
	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe rethrowing of the return value.
	 * 
	 * @param ex
	 * @return
	 */
	protected RuntimeException potentiallyConvertRuntimeException(Exception ex) {
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(new RuntimeException(ex));
		return resolved == null ? new RuntimeException(ex) : resolved;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeKeyspace()
	 */
	public String describeKeyspace() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#createColumnFamily(java.lang.String)
	 */
	public void createColumnFamily(String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropColumnFamily(java.lang.String)
	 */
	public void dropColumnFamily(String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findById(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> T findById(Object id, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findAll(java.lang.Class, java.lang.String)
	 */
	public <T> List<T> findAll(Class<T> entityClass, String columnFamilyName) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findByCQL(java.lang.String, java.lang.Class, java.lang.String)
	 */
	public <T> List<T> findByCQL(String cql, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void insert(T objectToSave, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void insert(Collection<T> batchToSave, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#save(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void save(T objectToSave, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#save(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void save(Collection<T> batchToSave, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void remove(T objectToRemove, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void remove(Collection<T> batchToRemove, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List, java.lang.Class, java.lang.String)
	 */
	public <T> void delete(List<String> rowKeys, Class<T> entityClass,
			String columnFamilyName) {
		// TODO Auto-generated method stub
		
	}
}
