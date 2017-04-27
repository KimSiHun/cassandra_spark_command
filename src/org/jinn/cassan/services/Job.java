package org.jinn.cassan.services;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraRow;

public class Job extends Thread
{

	private Cluster				cluster;
	private Cluster				spark_cluster;
	private JavaSparkContext	sc;
	private int					id;
	private List<String>		queries;
	private boolean				suffle;

	public Job(Cluster cluster, Cluster spark_cluster, JavaSparkContext sc, int id, List<String> queries,
			boolean suffle)
	{
		this.cluster = cluster;
		this.spark_cluster = spark_cluster;
		this.sc = sc;
		this.id = id;
		this.queries = queries;
		this.suffle = suffle;
	}

	private void execute(Session session, JavaSparkContext sc, int id, String query)
	{
		String query_type = query.trim().substring(0, 6).toLowerCase().trim();
		if (query_type.equals("spark,"))
		{
			query_type = "spark";
			if (null == sc)
			{
				return;
			}
		}
		long time_elapsed = 0L;
		long rs_count = 0L;
		String arr[] = null;
		try
		{
			long s_time = System.currentTimeMillis();
			if (query_type.equals("select"))
			{
				ResultSet rs = session.execute(query);
				for (Row r : rs)
				{
					if (null != r)
					{
						rs_count++;
					}
				}
			} else if (query_type.equals("spark"))
			{
				arr = query.replace(";", "").split("\\s*,\\s*");
				JavaRDD<CassandraRow> data = null;
				if (arr.length >= 5)
				{
					data = javaFunctions(sc).cassandraTable(arr[1], arr[2]).where(arr[3], arr[4])
							.limit(Long.parseLong(arr[5]));
				} else if (arr.length >= 3)
				{
					data = javaFunctions(sc).cassandraTable(arr[1], arr[2]);
				}

				rs_count = data.count();
			}
			long e_time = System.currentTimeMillis();
			time_elapsed = e_time - s_time;

			System.out.println("The elapsed time for the query '" + query_type + "': " + time_elapsed / 1000.0 + "s, "
					+ rs_count + " rows, Query: " + query);

		} catch (Exception e)
		{
			String msg = "Query ID:" + query_type + ", Query: " + query + " - " + e.toString();
			System.err.println(msg);
		}
	}

	@Override
	public void run()
	{
		int executed = 0;
		long s_time = System.currentTimeMillis();

		Session session = cluster.newSession();
		
		int idx = 0;
		int size = queries.size();
		int start_index = (int) (Math.random() * size);
		String query = null;
		if (size > 0)
		{
			for (int i = 0; i < size; i++)
			{
				if (suffle)
				{
					query = queries.get((i + start_index) % size);
				} else
				{
					query = queries.get(i);
				}
				System.out.println(Job.currentThread().getName() + " " + query);

				execute(session, sc, idx, query);
				executed++;
			}
		}

		long e_time = System.currentTimeMillis();
		long time_elapsed = (e_time - s_time);
		session.close();
		System.out.println(id + ": " + time_elapsed / 1000.0 + "s, " + executed);
	}
}
