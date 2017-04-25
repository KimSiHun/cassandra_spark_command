# Connecting

**using Java**
***
cassandra - maven  
 
`<dependency>`  
`<groupId>`com.datastax.cassandra`</groupId>`  
`<artifactId>`cassandra-driver-core`</artifactId>`  
`<version>`3.1.0`</version>`  
`</dependency>`  
`<dependency>`  
`<groupId>`com.datastax.cassandra`</groupId>`  
`<artifactId>`cassandra-driver-mapping`</artifactId>`  
`<version>`3.1.0`</version>`  
`</dependency>`  

  
1. **basic** use _Cluster & session_
    
<pre><code>
import com.datastax.driver.core.Cluster;<br>
import com.datastax.driver.core.Session;<br>

public Cluster get_cluster(){
	return Cluster.builder().addContactPoint("127.0.0.1").withPoolingOptions(pool).build();
}

public Session get_session(Cluster cluster, String key_space){
	return Cluster.connect(key_space);
}

</code></pre>

