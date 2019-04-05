package Unit8;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ProjectName: EasyKafkaDemos
 * @Package: Unit8
 * @ClassName: CustomerFileStreamSinkConnector
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-5 上午9:48
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-5 上午9:48
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class CustomerFileStreamSinkConnector extends SinkConnector {
    //声明文件配置变量
    public static final String FILE_CONFIG = "file";
    //实例化一个配置对象
    private static final ConfigDef CONFIG_DEF = new ConfigDef().define(FILE_CONFIG,
            ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,"Destination filename.");
    //声明一个文件名变量
    private String filename;

    //获取版本信息
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    //初始化
    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
    }

    //指定按照哪个类输出
    @Override
    public Class<? extends Task> taskClass() {
        return CustomerFileStreamSinkTask.class;
    }

    //获取配置信息
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String,String>> configs = new ArrayList<>();
        for(int i = 0; i < maxTasks;i++){
            Map<String,String> config = new HashMap<>();
            if(filename != null)
                config.put(FILE_CONFIG,filename);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
