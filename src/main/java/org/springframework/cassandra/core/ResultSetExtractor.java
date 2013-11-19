package org.springframework.cassandra.core;

import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.DriverException;

public interface ResultSetExtractor<T> {

	T extractData(ResultSet rs) throws DriverException, DataAccessException;
}
