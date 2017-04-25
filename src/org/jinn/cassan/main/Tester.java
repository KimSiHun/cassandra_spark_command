package org.jinn.cassan.main;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.jinn.cassan.conf.Configure;
import org.jinn.cassan.connection.Connection;
import org.jinn.cassan.services.Job;
import org.jinn.cassan.services.ReadQuery;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Tester
{

	private static final long		PR_START_TIME	= System.currentTimeMillis();
	private static final String		PID				= ManagementFactory.getRuntimeMXBean().getName();

	private static Connection		con				= new Connection();
	private static String			sql_dir_path	= Configure.SQL_DIR_PATH;
	private static File				sql_dir			= Configure.sql_dir();

	private static String			cs_host			= Configure.get_conf_value("cs.host");
	private static String			sp_host			= Configure.get_conf_value("sp.host");
	private static String			sp_dc			= Configure.get_conf_value("sp.dc");
	private static String			keyspace		= Configure.get_conf_value("cs.keyspace");
	private static int				test_threads	= Integer.parseInt(Configure.get_conf_value("tester.threads"));

	private static Cluster			cluster			= null;
	private static Session			session			= null;
	private static Cluster			spark_cluster	= null;
	private static Session			spark_session	= null;
	private static JavaSparkContext	sc				= null;

	private static SimpleDateFormat	mms				= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

	private static void termination_check()
	{
		con.spark_discon(sc);
		con.disConnection(spark_cluster);
		con.disConnection(cluster);
	}

	public static void main(String[] args)
	{
		System.out.println("System - " + PID + "start at : " + mms.format(new Date(PR_START_TIME)));

		if (null == cs_host)
		{
			System.out.println("check configure file cassandra.host");
			System.exit(0);
		}
		cluster = con.get_cluster(cs_host);

		// conf spark set is null DC1 is all
		if (!sp_host.isEmpty())
		{
			spark_cluster = con.get_spark_cluster(sp_host);
			sc = con.spark_con(sp_host, sp_dc);
		} else
		{
			spark_cluster = cluster;
			sc = con.spark_con(cs_host, null);
		}

		// if shut down hook close all connection
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			@Override
			public void run()
			{
				termination_check();
			}
		});

		// get queries
		ReadQuery rq = new ReadQuery();
		List<String> queries = rq.read_queries(keyspace, sql_dir_path, sql_dir);

		if (queries.size() > 0)
		{
			long s = System.currentTimeMillis();

			Job jobs[] = new Job[test_threads];
			// threads set
			for (int i = 0; i < test_threads; i++)
			{
				jobs[i] = new Job(cluster, sc, i, queries);
				jobs[i].start();
			}
			
			for (Job j : jobs)
			{
				try
				{
					j.join();
				} catch (InterruptedException e1)
				{
					System.err.println(e1);
				}
			}

			long e = System.currentTimeMillis();
			System.out.println("the elapsed time for one loop: " + (e - s) / 1000.0 + "s");
		} else
		{
			System.out.println("check query file. query size : " + queries.size());
		}

	}

}
