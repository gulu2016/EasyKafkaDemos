package Unit8;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

/**
 * @ProjectName: EasyKafkaDemos
 * @Package: Unit8
 * @ClassName: CustomerFileStreamSinkTask
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-4-5 上午9:42
 * @UpdateUser: 更新者
 * @UpdateDate: 19-4-5 上午9:42
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class CustomerFileStreamSinkTask extends SinkTask {
    //声明一个日志对象(利用工厂模式创建对象)
    private static final Logger LOG =
            LoggerFactory.getLogger(CustomerFileStreamSinkTask.class);

    //声明一个文件名变量
    private String filename;
    //声明一个输出流对象
    private PrintStream outputStream;

    //构造函数
    public CustomerFileStreamSinkTask(){}

    //带参数的构造函数
    public CustomerFileStreamSinkTask(PrintStream outputStream){
        filename = null;
        this.outputStream = outputStream;
    }

    @Override
    public String version() {
        return new CustomerFileStreamSinkConnector().version();
    }

    //开始执行任务
    @Override
    public void start(Map<String, String> props) {
        filename = props.get(CustomerFileStreamSinkConnector.FILE_CONFIG);
        if(filename == null)
            outputStream = System.out;
        else {
            try {
                outputStream = new PrintStream(new FileOutputStream(filename,true),
                        false, StandardCharsets.UTF_8.name());
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    //发送记录给Sink并输出
    @Override
    public void put(Collection<SinkRecord> records) {
        for(SinkRecord record:records){
            LOG.trace("Waiting line to {}:{}",logFilename(),record.value());
            outputStream.println(record.value());
        }
    }

    //持久化数据
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOG.trace("Flushing output stream for {}",logFilename());
        outputStream.flush();
    }

    //停止任务
    @Override
    public void stop() {
        if(outputStream != null && outputStream != System.out)
            outputStream.close();
    }

    //判断是标准输出还是文件写入
    private String logFilename() {
        return filename == null?"stdout":filename;
    }
}
