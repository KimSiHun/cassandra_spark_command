#connecting

** using Java **
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
</code></pre>

