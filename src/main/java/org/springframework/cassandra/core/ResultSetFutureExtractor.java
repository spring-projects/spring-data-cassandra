package org.springframework.cassandra.core;

import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.DriverException;

public interface ResultSetFutureExtractor<T> {

	T extractData(ResultSetFuture rs) throws DriverException, DataAccessException;
}
