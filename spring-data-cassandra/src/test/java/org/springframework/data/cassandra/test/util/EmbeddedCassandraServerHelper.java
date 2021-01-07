/*
 * Copyright 2017-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.util;

import static java.util.concurrent.TimeUnit.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.FileCopyUtils;
import org.springframework.util.FileSystemUtils;

/**
 * Imported Embedded Cassandra server startup helper.
 *
 * @author Mark Paluch
 */
class EmbeddedCassandraServerHelper {

	private static Logger log = LoggerFactory.getLogger(EmbeddedCassandraServerHelper.class);

	public static final long DEFAULT_STARTUP_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(20);
	private static final String DEFAULT_TMP_DIR = "target/embeddedCassandra";

	private static final AtomicReference<Object> sync = new AtomicReference<>();
	private static final AtomicReference<CassandraDaemon> cassandraRef = new AtomicReference<>();

	private static String launchedYamlFile;

	/**
	 * Get the embedded cassandra cluster name
	 *
	 * @return the cluster name
	 */
	public static String getClusterName() {
		return DatabaseDescriptor.getClusterName();
	}

	/**
	 * Get embedded cassandra host.
	 *
	 * @return the cassandra host
	 */
	public static String getHost() {
		return DatabaseDescriptor.getRpcAddress().getHostName();
	}

	/**
	 * Get embedded cassandra native transport port.
	 *
	 * @return the cassandra native transport port.
	 */
	public static int getNativeTransportPort() {
		return DatabaseDescriptor.getNativeTransportPort();
	}

	/**
	 * Get embedded cassandra RPC port.
	 *
	 * @return the cassandra RPC port
	 */
	public static int getRpcPort() {
		return DatabaseDescriptor.getRpcPort();
	}

	/**
	 * Start an embedded Cassandra instance.
	 *
	 * @param yamlResource
	 * @param timeout
	 * @throws Exception
	 */
	public static void startEmbeddedCassandra(String yamlResource, long timeout) throws Exception {
		startEmbeddedCassandra(yamlResource, DEFAULT_TMP_DIR, timeout);
	}

	/**
	 * Start an embedded Cassandra instance.
	 *
	 * @param yamlResource
	 * @param tmpDir
	 * @param timeout
	 * @throws Exception
	 */
	private static void startEmbeddedCassandra(String yamlResource, String tmpDir, long timeout) throws Exception {

		if (cassandraRef.get() != null) {
			/* Nothing to do; Cassandra is already started */
			return;
		}

		if (!sync.compareAndSet(null, new Object())) {
			/* A different Thread was faster, so nothing to do this time */
			return;
		}

		File yamlFile = new File(tmpDir, new File(yamlResource).getName());

		prepareCassandraDirectory(yamlResource, tmpDir, yamlFile);
		startEmbeddedCassandra(yamlFile, timeout);
	}

	/**
	 * Cleanup directory, copy YAML file to configuration directory and
	 *
	 * @param yamlFileName
	 * @param cassandraDirectoryName
	 * @param yamlFile
	 * @throws IOException
	 */
	private static void prepareCassandraDirectory(String yamlFileName, String cassandraDirectoryName, File yamlFile)
			throws IOException {

		File cassandraDirectory = new File(cassandraDirectoryName);

		rmdirs(cassandraDirectory);
		copy(yamlFileName, cassandraDirectory);
	}

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 */
	private static void startEmbeddedCassandra(File file, long timeout) throws Exception {

		checkConfigNameForRestart(file.getAbsolutePath());

		log.debug("Starting cassandra...");
		log.debug("Initialization needed");

		System.setProperty("cassandra.config", "file:" + file.getAbsolutePath());
		System.setProperty("cassandra-foreground", "true");
		System.setProperty("cassandra.native.epoll.enabled", "false"); // JNA doesn't cope with relocated netty

		cleanupAndRecreateDirectories();

		CassandraDaemon cassandraDaemon = new CassandraDaemon();

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Future<?> future = executor.submit(() -> {
			cassandraDaemon.activate();
			cassandraRef.compareAndSet(null, cassandraDaemon);
		});

		try {
			future.get(timeout, MILLISECONDS);
		} catch (ExecutionException cause) {

			log.error("Cassandra daemon did not start after " + timeout + " ms. Consider increasing the timeout");

			throw new IllegalStateException("Cassandra daemon did not start within timeout", cause);
		} catch (InterruptedException cause) {

			log.error("Interrupted waiting for Cassandra daemon to start:", cause);
			Thread.currentThread().interrupt();

			throw new IllegalStateException(cause);
		} finally {
			executor.shutdown();
		}
	}

	private static void cleanupAndRecreateDirectories() throws IOException {

		DatabaseDescriptor.daemonInitialization();
		createCassandraDirectories();
		cleanup();
		createCassandraDirectories();

		CommitLog commitLog = CommitLog.instance;

		commitLog.getCurrentPosition(); // wait for commit log allocator instantiation to avoid hanging on a race condition
		commitLog.resetUnsafe(true); // cleanup screws w/ CommitLog, this brings it back to safe state
	}

	private static void cleanup() throws IOException {

		// clean up commit log and data locations
		rmdirs(DatabaseDescriptor.getCommitLogLocation());
		rmdirs(DatabaseDescriptor.getAllDataFileLocations());
	}

	private static void checkConfigNameForRestart(String yamlFile) {

		boolean wasPreviouslyLaunched = cassandraRef.get() != null;

		if (wasPreviouslyLaunched && !launchedYamlFile.equals(yamlFile)) {
			throw new UnsupportedOperationException("We can't launch two Cassandra configurations in the same JVM instance");
		}

		launchedYamlFile = yamlFile;
	}

	/**
	 * Copies a resource from within the jar to a directory.
	 *
	 * @param resource name of the resource
	 * @param targetDirectory name of the target directory
	 * @throws IOException
	 */
	private static void copy(String resource, File targetDirectory) throws IOException {

		targetDirectory.mkdirs();

		File file = new File(targetDirectory, new File(resource).getName());
		InputStream is = EmbeddedCassandraServerHelper.class.getClassLoader().getResourceAsStream(resource);
		OutputStream out = new FileOutputStream(file);

		FileCopyUtils.copy(is, out);

		out.close();
		is.close();
	}

	private static void createCassandraDirectories() {
		DatabaseDescriptor.createAllDirectories();
	}

	private static void rmdirs(String... fileOrDirectories) throws IOException {

		for (String fileOrDirectory : fileOrDirectories) {
			rmdirs(new File(fileOrDirectory));
		}
	}

	private static void rmdirs(File... fileOrDirectories) throws IOException {

		Arrays.stream(fileOrDirectories).filter(File::exists).forEach(FileSystemUtils::deleteRecursively);
	}
}
