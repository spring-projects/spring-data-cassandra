# Spring Data Cassandra Project Info

## Release Preview

The goal of this release preview is to publish the pieces of spring-data-cassandra as they become available
so that user's of the module can start to familiarize themselves with the components, and ultimately to provide
the development team feedback.  We hope this iterative approach produces the most usable and developer friendly
spring-data-cassandra module.

### What's included (Q4 - 2013 BUILD-SNAPSHOT)

#### spring-cassandra module

This is the core template framework just like the one you are used to using on all your Spring Data projects.  Our
``CasandraTemplate`` provides everything you need for working with Cassandra using the Template pattern from Spring.

This includes ExceptionTranslation, JavaConfig and XML Configuration support.  Define your spring beans to setup your
Cassandra ``Cluster`` object, then neatly create your ``Session`` and you are ready to interact with Cassandra using our 
very flexible Template.

#### Best practices

We have worked closely with the DataStax Driver Engineering team to ensure that our implementation around their native
CQL Driver takes advantage of all that it has to offer. If you need access to more than one Keyspace in your application,
create more than one ``CassandraTemplate`` (one for each Keyspace), then use the appropriate Template where needed.

Here are some considerations when designing your application for spring-cassandra.

* When creating a Template, wire in a single Session for single Keyspace.  _Use one Template per Keyspace_
* Do not issue ``USE <keyspace>`` commands on your session.
* Cassandra ``Session`` object is thread-safe and you only need one.
* The DataStax Java Driver handles all fail over and retry logic for you.  Become familiar with [Driver Documentation](http://www.datastax.com/documentation/developer/java-driver/1.0/webhelp/index.html) which will help you configure your ``Cluster``.
* If you are using a Cassandra Cluster spanning multiple data centers, please be insure to include hosts from both data centers in your Contact Points.

#### High Performance Ingestion

We have included a variety of overloaded ``ingest()`` methods in the Template for high performance batch writes.

### What's Next (Q1 - 2014)

#### Cassandra Admin Template

This is another Template to help you with all of your Keyspace and Table administration.  We include a full CqlBuilder compatible
with all the options available in Cassandra.

#### Cassandra Data Template

This Template extends the ``CassnadraTemplate`` to provide even more interaction with Cassandra using annotated POJOs.
The ``CassandraDataTemplate`` is the customer of the Repository interface that will complete the spring-data-cassandra module.
This _DataTemplate_ gives the developer the luxury of working with POJOs and the Template pattern without using the Repository interface.

#### Official Reference Guide

Once we have all the inner workings of the Repository interface completed, we will publish a full Reference Guide on using all of
the features in spring-data-cassandra.

## Examples

### JavaConfig

Here is a very basic example to get your projected connected to Cassandra 1.2 running on your local machine.

	@Configuration
	public class TestConfig extends AbstractCassandraConfiguration {

		public static final String keyspace = "test";

		/* (non-Javadoc)
	 	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#getKeyspaceName()
	 	*/
		@Override
		protected String getKeyspaceName() {
			return keyspace;
		}

		/* (non-Javadoc)
	 	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#cluster()
	 	*/
		@Override
		@Bean
		public Cluster cluster() {

			Builder builder = Cluster.builder();
			builder.addContactPoint("127.0.0.1");
			return builder.build();
		}

		@Bean
		public SessionFactoryBean sessionFactoryBean() {

			SessionFactoryBean bean = new SessionFactoryBean(cluster(), getKeyspaceName());
			return bean;

		}

		@Bean
		public CassandraOperations cassandraTemplate() {

			CassandraOperations template = new CassandraTemplate(sessionFactoryBean().getObject());
			return template;
		}

	}

### XML Configuration

	<cassandra-cluster/>
	<cassandra-keyspace name="test"/>
	<cassandra-session/>
	<cassandra-template/>

### Using the Template

	public class CassandraDataOperationsTest {

		@Autowired
		private CassandraOperations cassandraTemplate;
		
		public Integer getCount() throws DataAccessException {
		
			String cql = "select count(*) from table_name";
			
			Integer count = cassandraTemplate.queryForObject(cql, Integer.class);
			
			log.info("Row Count is -> " + count);
			
			return count;
			
		}
	}

## Current Status

This community-led [Spring Data](http://projects.spring.io/spring-data)
subproject is well underway.

A first milestone (M1) is expected sometime in early 1Q14 built on the
following artifacts (or more recent versions thereof):

* Spring Data Commons 1.7.x
* Cassandra 1.2
* Datastax Java Driver 1.x
* JDK 1.6+

The GA release is expected as part of the as-yet unnamed fourth Spring
Data Release Train "D", following Spring Data Release Train
[Codd](https://github.com/spring-projects/spring-data-commons/wiki/Release-Train-Codd).


## Cassandra 2.x

We are anticipating support for Cassandra 2.x and Datastax Java Driver
2.x in a parallel branch after the 1.x-based support has been
released.


## Source Repository & Issue Tracking

Source for this module is hosted on GitHub in [Spring Projects](https://github.com/spring-projects).  
The Spring Data Cassandra JIRA can be found [here](https://jira.springsource.org/browse/DATACASS).

## Reporting Problems

If you encounter a problem using this module, please report the bug to us using [JIRA](https://jira.springsource.org/browse/DATACASS).  You
will need to create an account to log bugs.  Please provide as much information as you can, including:

* Cassandra Version
* Community or DataStax distribution of Cassandra
* JDK Version
* Output of ``show schema`` where applicable
* Any stacktraces showing the location of the problem
* Any other relevant information that you would like to see if you were diagnosing the problem.

Please do not post anything to Jira or the Mailing list on how to use Cassandra.  That is beyond the scope
of this module and there are a variety of resources available targeting that specific subject.

We recommend reading the following:

* [PlanetCassandra.org](http://planetcassandra.org/)
* [Getting Started](http://wiki.apache.org/cassandra/GettingStarted)
* [Driver Documentation](http://www.datastax.com/documentation/developer/java-driver/1.0/webhelp/index.html)


## Contact

For more information, feel free to contact the individuals listed
below:

* David Webb:  dwebb _at_ prowaveconsulting _dot_ com
* Matthew Adams:  matthew _dot_ adams _at_ scispike _dot_ com

## Sponsoring Companies

Spring Data Cassandra is being led and supported by the following
companies and individuals:

* [Prowave Consulting](http://www.prowaveconsulting.com)
* [SciSpike](http://www.scispike.com)
* [VHA](http://www.vha.com)
* Alexander Shvid

The following companies and individuals are also generously providing
support:

* [DataStax](http://www.datastax.com)
* [Spring](http://www.spring.io) @ [Pivotal](http://www.gopivotal.com)
