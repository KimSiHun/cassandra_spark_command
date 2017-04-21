package org.jinn.cassan.connection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.jinn.cassan.conf.Configure;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Connection
{

	private String	cs_dc;
	private String	cs_host;
	private String	cs_keyspace;

	public Connection()
	{
		cs_dc = Configure.get_conf_value("cs.dc");
		cs_host = Configure.get_conf_value("cs.host");
		cs_keyspace = Configure.get_conf_value("cs.keyspace");
	}

	public Cluster get_cluster()
	{
		return Cluster.builder().addContactPoint(cs_host).build();
	}

	public Session get_session(Cluster cluster)
	{
		Session session = cluster.connect(cs_keyspace);

		return session;
	}

	public void disConnection(Cluster cluster, Session session)
	{
		session.close();
		cluster.close();
	}

	public SparkConf spark_configure()
	{
		SparkConf spark_conf = null;
		String cassandra_url = cs_host;
		if (null != cassandra_url)
		{
			spark_conf = new SparkConf(true).setAppName("tester").set("spark.cassandra.auth.username", "cassandra")
					.set("spark.cassandra.auth.password", "cassandra").setMaster("local")
					.set("spark.cassandra.connection.host", cs_host);
			if (null != cs_dc)
			{
				spark_conf.set("spark.cassandra.connection.local_dc", cs_dc);
			}
		}
		return spark_conf;
	}

	public JavaSparkContext spark_con()
	{
		SparkConf spark_conf = spark_configure();
		JavaSparkContext sc = new JavaSparkContext(spark_conf);

		return sc;
	}

	public void spark_discon(JavaSparkContext sc)
	{
		sc.stop();
		sc.close();
	}
}
