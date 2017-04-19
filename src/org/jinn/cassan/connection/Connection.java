package org.jinn.cassan.connection;

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
	
	public void disConnection(Cluster cluster,Session session)
	{
		session.close();
		cluster.close();
	}
}
