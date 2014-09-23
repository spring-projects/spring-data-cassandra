package org.springframework.cassandra.test.integration.core.template.async;

import java.util.Map;

import org.springframework.cassandra.core.QueryForListOfMapListener;

public class ListOfMapListener extends ListListener<Map<String, Object>> implements QueryForListOfMapListener {}
