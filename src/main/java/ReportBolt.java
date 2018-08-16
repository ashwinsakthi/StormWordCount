import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.shade.com.google.common.collect.Maps;

public class ReportBolt extends BaseRichBolt 
{
	private OutputCollector collector;
    private JdbcClient jdbcClient;
    private ConnectionProvider connectionProvider;

    private HashMap<String, Long> ReportCounts = new HashMap<String,Long>();;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) 
    {
        this.ReportCounts = new HashMap<String, Long>();
        this.collector = collector;
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/wordcount");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","test");
        connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        //对数据库连接池进行初始化
        connectionProvider.prepare();
        jdbcClient = new JdbcClient(connectionProvider, 30);
    }

    public void execute(Tuple tuple) 
    {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.ReportCounts.put(word, count);
        
        List<Column> list = new ArrayList();
        list.add(new Column("word", word, Types.VARCHAR));
        List<List<Column>> select = jdbcClient.select("select word from wordcount where word = ?",list);

        Long n = select.stream().count();
        if(n>=1){
            //update
            jdbcClient.executeSql("update wordcount set count = "+count+" where word = '"+word+"'");

        }else{
            //insert
            jdbcClient.executeSql("insert into wordcount values( '"+word+"',"+count+")");

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        // this bolt does not emit anything

    }
    public void cleanup() 
    {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.ReportCounts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.ReportCounts.get(key));
        }
        connectionProvider.cleanup();
        System.out.println("--------------");
    }
}
