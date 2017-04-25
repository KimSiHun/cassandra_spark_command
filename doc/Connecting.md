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
import com.datastax.driver.core.ResultSet;<br>

public class Test{

	private static Cluster get_cluster(){
		return Cluster.builder().addContactPoint("127.0.0.1").build();
	}

	public static void main(String[] args){
		String key_space = "test";
		Cluster cluster = get_cluster();
		Session session = cluster.connect(key_space);
		
		String query = "select * from test.test_table;"
		ResultSet rs = session.execute(query);
		
		int rs_count = rs.count();
	}
}
</code></pre>

