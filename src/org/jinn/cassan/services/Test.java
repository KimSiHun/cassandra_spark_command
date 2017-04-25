package org.jinn.cassan.services;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jinn.cassan.conf.Configure;
import org.jinn.cassan.connection.Connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraRow;

public class Test
{
	Connection				con;
	String					test_dir_path;
	File					sql_dir;

	private List<String>	queries;
	private List<String>	spark_queires;

	public Test()
	{
		con = new Connection();
		test_dir_path = Configure.get_conf_value("cs.keyspace");
		sql_dir = Configure.sql_dir();
		spark_queires = new ArrayList<String>();
		queries = new ArrayList<String>();
	}

	public void read_queries()
	{
		File test_dir = null;

		if (sql_dir.isDirectory())
		{
			test_dir = new File(sql_dir, test_dir_path);
		}

		String query_files[] = test_dir.list();
		File sql_file = null;
		BufferedReader br;
		StringBuilder sb = new StringBuilder();
		String query = null;
		for (String q : query_files)
		{
			sql_file = new File(test_dir, q);

			try
			{
				br = new BufferedReader(new InputStreamReader(new FileInputStream(sql_file), "UTF-8"));
				String line;
				while ((line = br.readLine()) != null)
				{
					if (line.startsWith("--"))
					{
						continue;
					}
					sb.append(line);
					if (line.endsWith(";"))
					{
						query = new String(sb);
						if (query.startsWith("spark,"))
						{
							spark_queires.add(query.trim());
						} else
						{
							queries.add(query.trim());
						}
						sb.setLength(0);
					}
				}
				br.close();
			} catch (IOException ioe)
			{
				System.err.println("System sql file read fail - " + ioe);
			}
		}

	}

	public void test()
	{
		Cluster cluster = con.get_cluster(Configure.get_conf_value("cs.host"));
		Session session = con.get_session(cluster, test_dir_path);

		for (String q : queries)
		{
			long s = System.currentTimeMillis();
			int rs_count = 0;
			ResultSet rs = session.execute(q);
			System.out.println(q + " " + rs);
			for (Row r : rs)
			{
				rs_count++;
				System.out.println(r);
			}
			long e = System.currentTimeMillis();
			System.out.println((e - s) + "ms rs_count: " + rs_count);
		}
		session.close();
		con.disConnection(cluster);
	}

	public void spark_test()
	{
		Cluster cluster = con.get_spark_cluster(Configure.get_conf_value("sp.host"));
		Session session = con.get_session(cluster, test_dir_path);

		JavaSparkContext sc = con.spark_con(Configure.get_conf_value("sp.host"), Configure.get_conf_value("sp.dc"));
		JavaRDD<CassandraRow> data = null;
		String arr[] = null;

		long rs_count = 0L;

		for (String sq : spark_queires)
		{
			long s = System.currentTimeMillis();
			arr = sq.split("\\s*,\\s*");

			if (arr[0].equals("spark,"))
			{
				arr[0] = "spark";
			}
			if (arr.length >= 5)
			{
				data = javaFunctions(sc).cassandraTable(arr[1], arr[2]).where(arr[3], Integer.parseInt(arr[4]))
						.limit(Long.parseLong(arr[5].replace(";", "")));
			} else if (arr.length >= 3)
			{
				data = javaFunctions(sc).cassandraTable(arr[1], arr[2]);
			}

			rs_count = data.count();

			long e = System.currentTimeMillis();
			System.out.println((e - s) + "ms rs_count:" + rs_count);

		}
		session.close();
		con.disConnection(cluster);
		con.spark_discon(sc);
	}

}
