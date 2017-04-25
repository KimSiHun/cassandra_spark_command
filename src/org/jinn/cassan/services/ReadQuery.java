package org.jinn.cassan.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ReadQuery
{
	private static String sql_suffix = ".sql";

	public List<String> read_queries(String test_dir_path, String sql_dir_path, File sql_dir)
	{
		// test_sql_dir get
		File test_dir = null;
		if (null != sql_dir_path)
		{
			test_dir = new File(sql_dir_path + "/" + test_dir_path);
		} else
		{
			test_dir = new File(sql_dir, test_dir_path);
		}
		// test_sql_file get <<keyspace>>.sql
		File test_file = null;
		if (test_dir.isDirectory())
		{
			test_file = new File(test_dir.getAbsolutePath() + "/" + test_dir_path + sql_suffix);
		}

		if (!test_file.isFile())
		{
			System.out.println("Check query File exist or name is failed - " + test_file.getAbsolutePath());
			System.exit(0);
		}

		// read sql file return List queries
		BufferedReader br = null;
		String line = null;
		StringBuilder sb = new StringBuilder();
		List<String> queries = new ArrayList<String>();
		try
		{
			br = new BufferedReader(new InputStreamReader(new FileInputStream(test_file), "utf-8"));
			int idx = 0;
			while ((line = br.readLine()) != null)
			{
				idx = line.indexOf("--");
				if (idx == 0)
				{
					continue;
				} else if (idx > 0 && line.indexOf(';') > 0)
				{
					line = line.substring(0, idx);
				}
				if (line.trim().length() > 0)
				{
					if ((idx = line.indexOf(';')) >= 0)
					{
						sb.append(line);
						queries.add(new String(sb));
						sb.setLength(0);
					} else
					{
						sb.append(line);
						sb.append("\n");
					}
				}
			}
			br.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}

		return queries;
	}

}
