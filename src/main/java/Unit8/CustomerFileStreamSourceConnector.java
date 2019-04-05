package Unit8;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.net.ConnectException;
import java.util.*;

/**
 * @ProjectName: EasyKafkaDemos
 * @Package: Unit8
 * @ClassName: CustomerFileStreamSourceConnector
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-3 上午10:52
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-3 上午10:52
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class CustomerFileStreamSourceConnector extends SourceConnector {
    //定义主题配置变量
    public static final String TOPIC_CONFIG = "topic";
    //定义文件配置变量
    public static final String FILE_CONFIG = "file";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Source filename.")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,"The topic to publish data to");

    //声明文件名变量
    private String filename;
    //生命主题变量
    private String topic;

    //获取版本
    public String version() {
        return AppInfoParser.getVersion();
    }

    //开始初始化
    public void start(Map<String, String> props) {
        filename = props.get(TOPIC_CONFIG);
        topic = props.get(TOPIC_CONFIG);

        if(topic == null || topic.isEmpty())
            try {
                throw new ConnectException("FileStreamSourceConnector " +
                        "configuration must include 'topic' setting");
            } catch (ConnectException e) {
                e.printStackTrace();
            }
        if(topic.contains(","))
            try {
                throw new ConnectException("FileStreamSourceConnector " +
                        "should only have a single topic when used as a source");
            } catch (ConnectException e) {
                e.printStackTrace();
            }

    }


    public Class<? extends Task> taskClass() {
        return CustomerFileStreamSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String,String>> configs = new ArrayList<>();
        Map<String,String> config = new HashMap<>();
        if(filename != null)
            config.put(FILE_CONFIG,filename);
        config.put(TOPIC_CONFIG,topic);
        configs.add(config);
        return configs;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
