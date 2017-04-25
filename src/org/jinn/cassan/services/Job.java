package org.jinn.cassan.services;

import java.lang.management.ManagementFactory;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class Job extends Thread
{

	private Cluster				cluster;
	private Session				session;
	private JavaSparkContext	sc;
	private int					id;
	private List<String>		queries;

	public Job(Cluster cluster, JavaSparkContext sc, int id, List<String> queries)
	{
		this.cluster = cluster;
		this.sc = sc;
		this.id = id;
		this.queries = queries;
	}

	private void execute(Cluster cluster, Session session, JavaSparkContext sc, int id, String query)
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
				ResultSet rs = (ResultSet) session.execute(query);
				while (rs.next())
				{
					rs_count++;
				}
				rs.close();
			} else if (query_type.equals("spark"))
			{
				arr = query.split("\\s*,\\s*");
				JavaRDD<CassandraRow> data = null;
				if (arr.length >= 5)
				{
					data = javaFunctions(sc).cassandraTable(arr[1], arr[2]).where(arr[3], arr[4]);
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
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
		int executed = 0;
		long s_time = System.currentTimeMillis();

		Session session = cluster.newSession();

		int idx = 0;
		int size = queries.size();
		int start_index = (int)(Math.random()*size);
		String query =null;
		if (size>0)
		{
			for (int i = 0; i < size; i++)
			{
				query = queries.get(i);
				System.out.println(Job.currentThread().getName()+" "+query);
				
				
			}
		}
	}
}
