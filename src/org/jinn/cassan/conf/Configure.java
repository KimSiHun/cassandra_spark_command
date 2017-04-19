package org.jinn.cassan.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class Configure
{
	private static final String	ROOT_DIR_PATH	= System.getProperty("user.dir");
	private static final String	CONF_DIR_PATH	= ROOT_DIR_PATH + "/conf";
	private static final String	SQL_DIR_PATH	= ROOT_DIR_PATH + "/sql";

	private static File conf_dir()
	{
		return new File(CONF_DIR_PATH);
	}

	public static File sql_dir()
	{
		return new File(SQL_DIR_PATH);
	}

	private static Properties read_conf_file()
	{

		File conf_dir = conf_dir();
		File conf_file = null;
		if (null != conf_dir && conf_dir.isDirectory())
		{
			conf_file = new File(conf_dir + "/conf.properties");
		}

		Properties conf_properties = new Properties();
		try
		{
			InputStreamReader isr = new InputStreamReader(new FileInputStream(conf_file), "UTF-8");
			conf_properties.load(isr);
			isr.close();
		} catch (IOException ioe)
		{
			System.err.println(System.currentTimeMillis() + " - " + ioe + " - read_conf");
			System.exit(0);
		}
		return conf_properties;
	}

	public static String get_conf_value(String s)
	{

		String key = null, value = null;

		Properties conf_properties = read_conf_file();

		if (null != s && s.equals("cs.host"))
		{
			key = "cassandra.host";
		} else if (null != s && s.equals("cs.dc"))
		{
			key = "cassandra.dc";
		} else if (null != s && s.equals("cs.keyspace"))
		{
			key = "cassandra.keyspace";

		} else if (null != s && s.equals("tester.threads"))
		{
			key = "cassandra.tester.threads";
		}

		if (null != key)
		{
			value = conf_properties.getProperty(key).replaceAll("\"", "").trim();
		}

		if (null != value)
		{
			return value;
		}

		return null;
	}
}
