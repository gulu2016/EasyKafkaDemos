package Unit8;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

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


    public String version() {
        return null;
    }

    public void start(Map<String, String> map) {

    }

    public Class<? extends Task> taskClass() {
        return null;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        return null;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return null;
    }
}
