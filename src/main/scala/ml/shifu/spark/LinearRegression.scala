package ml.shifu.spark

import ml.shifu.shifu.container.obj.ColumnConfig
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.fs.PathFinder
import ml.shifu.shifu.util.{CommonUtils, Constants}
import ml.shifu.spark.data.PigLoader
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import scala.collection.JavaConverters._


/**
  * Created by Mark on 6/1/2017.
  */
object LinearRegression {

  def main(args: Array[String]): Unit = {

    val context = new ShifuContext(args)

    val spark = SparkSession.builder().appName("SHIFU-SPARK-LogisticRegression").getOrCreate()

    val modelConfig = CommonUtils.loadModelConfig(context.modelConfigPath.getOrElse(Constants.LOCAL_MODEL_CONFIG_JSON),
                                                  SourceType.HDFS)
    val pathFinder = new PathFinder(modelConfig)

    val data = spark.read.format("pig").load(pathFinder.getNormalizedDataPath)

    val columnConfigList = asScalaBufferConverter(
      CommonUtils.loadColumnConfigList(context.columnConfigPath.getOrElse(Constants.LOCAL_COLUMN_CONFIG_JSON),
                                                        SourceType.HDFS)
    ).asScala

    val selectName = columnConfigList.filter( c => c.isFinalSelect).map(c => c.getColumnName)

    //prepare data
    val pigData = new PigLoader(spark, columnConfigList)

    val training = data.select(selectName.head, selectName.tail:_*)

    val lr = new LogisticRegression().setMaxIter(modelConfig.getNumTrainEpochs)
                                     .setRegParam(0.3)
                                     .setElasticNetParam(0.8)
                                     .setLabelCol(modelConfig.getTargetColumnName)


    val model = lr.fit(training)

  }
}

class ShifuContext(args: Seq[String]) extends ScallopConf(args) {
  val modelConfigPath = opt[String](required = true)
  val columnConfigPath = opt[String](required = true)
}