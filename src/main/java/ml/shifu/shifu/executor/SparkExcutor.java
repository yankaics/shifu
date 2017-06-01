package ml.shifu.shifu.executor;

import ml.shifu.shifu.container.obj.ModelConfig;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Mark on 6/1/2017.
 */
public class SparkExcutor {

    private Logger LOG = LoggerFactory.getLogger(SparkExcutor.class);
    private SparkLauncher launcher;
    private ModelConfig config;

    public SparkExcutor(ModelConfig config) {
        launcher = new SparkLauncher();
        this.config = config;
    }

    public SparkLauncher setupName() {
        return launcher.setAppName("SHIFU-SPARK");
    }

    public SparkLauncher setupName(String name) {
        return launcher.setAppName(name);
    }

    public void setupMaster() {
        launcher.setMaster(config.isDistributedRunMode() ? "yarn" : "local[2]");
    }

    public int submit() {
        try {
            return launcher.launch().waitFor();
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage());
            return -1;
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
            return -1;
        }
    }
}
