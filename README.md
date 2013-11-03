# Spring Data Cassandra

The primary goal of the [Spring Data](http://www.springsource.org/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.

The Spring Data Cassandra project aims to provide a familiar and consistent Spring-based programming model for new datastores while retaining store-specific features and capabilities. The Spring Data Cassandra project provides integration with the Cassandra BigTable/Columnar database. Key functional areas of Spring Data Cassandra are a POJO centric model for interacting with a Cassandra Column Families and easily writing a repository style data access layer using the famailiar "Template" API.

## Recent Changes

The Spring Data Cassandra project is now using only the CQL DataStax Java Driver.  There is no longer Thrift Support.

## Contributing

Please contact David Webb (dwebb@prowaveconsulting.com) about contributing to this project.

## Getting Help

For a comprehensive list of all the Spring Data Cassandra features, please refer to:

* the [Spring Data Cassandra Jira](https://jira.springsource.org/browse/DATACASS) project.

If you are new to Spring as well as to Spring Data, look for information about [Spring projects](http://www.springsource.org/projects). 

## Quick Start

### Maven configuration

Add the Maven dependency:

    <dependency>
    	<groupId>org.springframework.data</groupId>
    	<artifactId>spring-data-cassandra</artifactId>
    	<version>${spring.cassandra.version}</version>
    </dependency>

### CassandraTemplate

CassandraTemplate is the central support class for Cassandra database operations. It provides:

* Basic POJO mapping support to and from Cassandra Column Families
* Convenience methods to interact with the store (insert object, update objects) 
* Full CQL Support
* Connection Management to the Cassandra Ring

### XML Configuration

Here is an example XML Configutation using the cassandra namespace

    <?xml version="1.0" encoding="UTF-8"?>
    <beans xmlns="http://www.springframework.org/schema/beans"
    	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    	xmlns:context="http://www.springframework.org/schema/context"
    	xmlns:cassandra="http://www.springframework.org/schema/data/cassandra"
    	xmlns:p="http://www.springframework.org/schema/p"
    	xsi:schemaLocation="http://www.springframework.org/schema/beans 
           	http://www.springframework.org/schema/beans/spring-beans-3.2.xsd 
           	http://www.springframework.org/schema/data/cassandra
           	http://www.springframework.org/schema/data/cassandra/spring-cassandra.xsd
           	http://www.springframework.org/schema/context 
           	http://www.springframework.org/schema/context/spring-context-3.2.xsd">
    
    	<context:component-scan base-package="com.yourdomain.package.with.springstuff" />
    
    	<cassandra:cassandra keyspace="myKeyspaceName"/>
    	<cassandra:template/>
    
    </beans>

### Java Configuration

Java @Configuration support is very straight forward as in this example:

    package org.springframework.data.cassandra.test.config;
    
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.List;
    
    import lombok.extern.log4j.Log4j;
    
    import org.springframework.context.annotation.Bean;
    import org.springframework.context.annotation.Configuration;
    import org.springframework.data.cassandra.core.CassandraConnectionFactoryBean;
    import org.springframework.data.cassandra.core.CassandraTemplate;
    
    /**
     * Setup any spring configuration for unit tests
     * 
     * @author David Webb
     *
     */
    @Log4j
    @Configuration
    public class TestConfig {
    	
    	public @Bean CassandraConnectionFactoryBean cassandra() {
    
    		List<String> seeds = new ArrayList<String>();
    		seeds.add("127.0.0.1");
    		
    		CassandraConnectionFactoryBean cfb = new CassandraConnectionFactoryBean();
    		
    		cfb.setSeeds(seeds);
    		cfb.setPort(9042);
    		
    		return cfb;
    	}
    	
    	public @Bean CassandraTemplate cassandraTemplate() {
    		CassandraTemplate template = null;
    		try {
    			template = new CassandraTemplate(cassandra().getObject());
    		} catch (Exception e) {
    			log.error(e);
    		}
    		return template;
    	}
    
    
    }

### Autowiring the template

Regardless of the configuration you use, just @Autowire the cassandraTemplate to start interacting with your Cassandra database.

    @Autowired
    private CassandraTemplate cassandraTemplate;

## Annotating POJOs for Cassandra

Spring Data Cassandra offers two main annotations for its automatic mapping of Java Objects to/from Cassandra Column Families.   Here is a simple example:

    package org.springframework.data.cassandra.test.cf;
    
    import lombok.Data;
    
    import org.springframework.data.annotation.Id;
    import org.springframework.data.cassandra.core.entitystore.Column;
    import org.springframework.data.cassandra.core.entitystore.ColumnFamily;
    
    /**
     * The Jobs Column Family for persisting to Cassandra
     * 
     * @author David Webb
     *
     */
    @ColumnFamily
    @Data
    public class Jobs {
    
    	@Id
    	private String key;
    	
    	@Column(name="job_title")
    	private String jobTitle;
    	
    	@Column(name="pay_rate")
    	private String payRate;
    
    }

The Column Family used will be the camelCase of the object class name, the Id will serve as the Cassandra Key column and the remaining attributes will be columns in the column family.  As is the case with Cassandra (the "V"ariability of the NoSQL 3 V's), you can modify your POJO as needed by your application without having to change the Cassandra column family.  The caveat here is that the key column (or at least the uniqueness of it's values) must remain the same.

## Contributing to Spring Data

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATACASS) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.

