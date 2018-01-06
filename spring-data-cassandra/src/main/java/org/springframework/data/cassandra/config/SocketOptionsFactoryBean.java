/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.Optional;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;

import com.datastax.driver.core.SocketOptions;

/**
 * Socket Options Factory Bean.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
@SuppressWarnings({ "unused", "WeakerAccess" })
public class SocketOptionsFactoryBean implements FactoryBean<SocketOptions>, InitializingBean {

	private @Nullable Integer connectTimeoutMillis;

	private @Nullable Boolean keepAlive;

	private @Nullable Integer readTimeoutMillis;

	private @Nullable Boolean reuseAddress;

	private @Nullable Integer soLinger;

	private @Nullable Boolean tcpNoDelay;

	private @Nullable Integer receiveBufferSize;

	private @Nullable Integer sendBufferSize;

	private @Nullable SocketOptions socketOptions;

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public SocketOptions getObject() throws Exception {
		return socketOptions;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<?> getObjectType() {
		return SocketOptions.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		this.socketOptions = new SocketOptions();

		Optional.ofNullable(this.connectTimeoutMillis).ifPresent(this.socketOptions::setConnectTimeoutMillis);
		Optional.ofNullable(this.readTimeoutMillis).ifPresent(this.socketOptions::setReadTimeoutMillis);
		Optional.ofNullable(this.keepAlive).ifPresent(this.socketOptions::setKeepAlive);
		Optional.ofNullable(this.reuseAddress).ifPresent(this.socketOptions::setReuseAddress);
		Optional.ofNullable(this.soLinger).ifPresent(this.socketOptions::setSoLinger);
		Optional.ofNullable(this.tcpNoDelay).ifPresent(this.socketOptions::setTcpNoDelay);
		Optional.ofNullable(this.receiveBufferSize).ifPresent(this.socketOptions::setReceiveBufferSize);
		Optional.ofNullable(this.sendBufferSize).ifPresent(this.socketOptions::setSendBufferSize);
	}

	@Nullable
	public Boolean getKeepAlive() {
		return keepAlive;
	}

	public void setKeepAlive(@Nullable Boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	@Nullable
	public Boolean getReuseAddress() {
		return reuseAddress;
	}

	public void setReuseAddress(@Nullable Boolean reuseAddress) {
		this.reuseAddress = reuseAddress;
	}

	@Nullable
	public Integer getSoLinger() {
		return soLinger;
	}

	public void setSoLinger(@Nullable Integer soLinger) {
		this.soLinger = soLinger;
	}

	@Nullable
	public Boolean getTcpNoDelay() {
		return tcpNoDelay;
	}

	public void setTcpNoDelay(@Nullable Boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}

	@Nullable
	public Integer getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public void setReceiveBufferSize(@Nullable Integer receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	@Nullable
	public Integer getSendBufferSize() {
		return sendBufferSize;
	}

	public void setSendBufferSize(@Nullable Integer sendBufferSize) {
		this.sendBufferSize = sendBufferSize;
	}

	/**
	 * @return Returns the connectTimeoutMillis.
	 */
	@Nullable
	public Integer getConnectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	/**
	 * @param connectTimeoutMillis The connectTimeoutMillis to set.
	 */
	public void setConnectTimeoutMillis(@Nullable Integer connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	/**
	 * @return Returns the readTimeoutMillis.
	 */
	@Nullable
	public Integer getReadTimeoutMillis() {
		return readTimeoutMillis;
	}

	/**
	 * @param readTimeoutMillis The readTimeoutMillis to set.
	 */
	public void setReadTimeoutMillis(@Nullable Integer readTimeoutMillis) {
		this.readTimeoutMillis = readTimeoutMillis;
	}

}
