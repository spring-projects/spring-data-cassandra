/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.config;

/**
 * Socket options.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class SocketOptionsConfig {

	private Integer connectTimeoutMls;
	private Boolean keepAlive;
	private Boolean reuseAddress;
	private Integer soLinger;
	private Boolean tcpNoDelay;
	private Integer receiveBufferSize;
	private Integer sendBufferSize;

	public Integer getConnectTimeoutMls() {
		return connectTimeoutMls;
	}

	public void setConnectTimeoutMls(Integer connectTimeoutMls) {
		this.connectTimeoutMls = connectTimeoutMls;
	}

	public Boolean getKeepAlive() {
		return keepAlive;
	}

	public void setKeepAlive(Boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public Boolean getReuseAddress() {
		return reuseAddress;
	}

	public void setReuseAddress(Boolean reuseAddress) {
		this.reuseAddress = reuseAddress;
	}

	public Integer getSoLinger() {
		return soLinger;
	}

	public void setSoLinger(Integer soLinger) {
		this.soLinger = soLinger;
	}

	public Boolean getTcpNoDelay() {
		return tcpNoDelay;
	}

	public void setTcpNoDelay(Boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}

	public Integer getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public void setReceiveBufferSize(Integer receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	public Integer getSendBufferSize() {
		return sendBufferSize;
	}

	public void setSendBufferSize(Integer sendBufferSize) {
		this.sendBufferSize = sendBufferSize;
	}
}
