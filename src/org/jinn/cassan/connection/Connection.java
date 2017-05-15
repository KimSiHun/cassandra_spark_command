package org.jinn.cassan.connection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;

public class Connection
{

	public Cluster get_cluster(String cs_host)
	{
		PoolingOptions pool = new PoolingOptions();
		pool.setPoolTimeoutMillis(15);
		pool.setHeartbeatIntervalSeconds(60);

		return Cluster.builder().addContactPoint(cs_host).withPoolingOptions(pool).build();
	}

	public Session get_session(Cluster cluster, String cs_keyspace)
	{
		Session session = cluster.connect(cs_keyspace);

		return session;
	}

	public void disConnection(Cluster cluster)
	{
		cluster.close();
	}

	public Cluster get_spark_cluster(String sp_host)
	{
		return Cluster.builder().addContactPoint(sp_host).build();
	}

	public SparkConf spark_configure(String sp_host, String sp_dc)
	{
		SparkConf spark_conf = null;
		String spark_url = sp_host;
		System.out.println(spark_url);
		if (null != spark_url)
		{
			spark_conf = new SparkConf(true).setAppName("cassandra").setMaster("local")
					.set("spark.cassandra.connection.host", spark_url).set("spark.executor.memory", "4g");
			if (null != sp_dc)
			{
				spark_conf.set("spark.cassandra.connection.local_dc", sp_dc);
			}
		}
		return spark_conf;
	}

	public JavaSparkContext spark_con(String sp_host, String sp_dc)
	{
		SparkConf spark_conf = spark_configure(sp_host, sp_dc);
		JavaSparkContext sc = new JavaSparkContext(spark_conf);

		return sc;
	}

	public void spark_discon(JavaSparkContext sc)
	{
		sc.stop();
		sc.close();
	}
}
