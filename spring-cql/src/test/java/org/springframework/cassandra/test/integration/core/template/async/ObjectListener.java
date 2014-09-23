package org.springframework.cassandra.test.integration.core.template.async;

import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.test.unit.support.TestListener;

class ObjectListener<T> extends TestListener implements QueryForObjectListener<T> {

	T result;
	Exception exception;

	@Override
	public void onQueryComplete(T result) {
		countDown();
		this.result = result;
	}

	@Override
	public void onException(Exception x) {
		countDown();
		this.exception = x;
	}
}
