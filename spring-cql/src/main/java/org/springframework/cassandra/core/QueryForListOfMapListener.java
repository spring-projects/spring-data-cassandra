package org.springframework.cassandra.core;

import java.util.Map;

/**
 * Listener used to receive asynchronous results expected as a <code>List&lt;T&gt;</code>.
 * 
 * @author Matthew T. Adams
 */
public interface QueryForListOfMapListener extends QueryForListListener<Map<String, Object>> {}
