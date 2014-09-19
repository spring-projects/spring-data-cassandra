package org.springframework.data.cassandra.repository.support;

import java.lang.reflect.Method;

public class IdInterfaceException extends RuntimeException {

	private static final long serialVersionUID = -1635695314254522703L;

	String idInterfaceName;
	String method;

	public IdInterfaceException(Class<?> idInterface, Method method, String message) {
		this(idInterface.getClass().getName(), method == null ? null : method.toString(), message);
	}

	public IdInterfaceException(String idInterfaceName, String method, String message) {
		super(message);
		this.idInterfaceName = idInterfaceName;
		this.method = method;
	}

	public String getIdInterfaceName() {
		return idInterfaceName;
	}

	public String getMethod() {
		return method;
	}
}
