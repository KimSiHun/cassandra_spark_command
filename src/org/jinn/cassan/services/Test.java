package org.jinn.cassan.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.jinn.cassan.conf.Configure;
import org.jinn.cassan.connection.Connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Test
{
	Connection				con;
	String					test_dir_path;
	File					sql_dir;

	private List<String>	queries;

	public Test()
	{
		con = new Connection();
		test_dir_path = Configure.get_conf_value("cs.keyspace");
		sql_dir = Configure.sql_dir();
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
						queries.add(query);
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
		Cluster cluster = con.get_cluster();
		Session session = con.get_session(cluster);

		ResultSet rs = session.execute(queries.get(0));
		
		for (Row r : rs)
		{
			System.out.println(r);
		}
		
		con.disConnection(cluster, session);
	}
}
