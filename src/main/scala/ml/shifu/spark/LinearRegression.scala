package ml.shifu.spark

import java.util

import ml.shifu.shifu.container.obj.{ColumnConfig, ModelConfig}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.container.obj.RawSourceData.SourceType.HDFS
import ml.shifu.shifu.fs.PathFinder
import ml.shifu.shifu.util.{CommonUtils, Constants}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Json
import org.rogach.scallop.ScallopConf


/**
  * Created by Mark on 6/1/2017.
  */
object LinearRegression {

  def main(args: Array[String]): Unit = {
    val context = new Context(args);
    val spark = SparkSession.builder().appName("SHIFU-SPARK-LogisticRegression").getOrCreate()
    val modelConfig = CommonUtils.loadModelConfig(context.modelConfigPath.getOrElse(Constants.LOCAL_MODEL_CONFIG_JSON),
                                                  SourceType.HDFS)
    val pathFinder = new PathFinder(modelConfig);
    val data = spark.read.format("pig").load(pathFinder.getNormalizedDataPath)

    //  val lr = new LogisticRegression().setMaxIter(modelConfig.getNumTrainEpochs)

    val columnConfig:util.List[ColumnConfig] =
      CommonUtils.loadColumnConfigList(context.columnConfigPath.getOrElse(Constants.LOCAL_COLUMN_CONFIG_JSON),
                                                        SourceType.HDFS)
    val finalSelect = columnConfig.stream().filter()

  }
}

class Context(args: Seq[String]) extends ScallopConf(args) {
  val modelConfigPath = opt[String](required = true)
  val columnConfigPath = opt[String](required = true)
}
