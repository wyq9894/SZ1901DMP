import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON

object Demo {
  def main(args: Array[String]): Unit = {
    val url = "{\"version\":\"3.0.0\",\"gauges\":{\"local-1575360686397.driver.Streaming-1.StreamingMetrics.streaming.lastReceivedBatch_processingStartTime\":{\"value\":1575360903014},\n\"local-1575360686397.driver.Streaming-1.StreamingMetrics.streaming.lastReceivedBatch_processingEndTime\":{\"value\":1575360903357},\"local-1575360686397.driver.BlockManager.memory.maxOnHeapMem_MB\":{\"value\":1445},\n\"local-1575360686397.driver.Streaming-1.StreamingMetrics.streaming.lastReceivedBatch_submissionTime\":{\"value\":1575360903013}}}"
    val json=JSON.parseObject(url)

    //读取网址json数据
    // val json= SparkMetricsUtils.getMetricsJson(url)


    val startTime = json.getJSONObject("gauges").getJSONObject("local-1575360686397.driver.Streaming-1.StreamingMetrics.streaming.lastReceivedBatch_processingStartTime")
      .getLong("value")

    val endTime=json.getJSONObject("gauges").getJSONObject("local-1575360686397.driver.Streaming-1.StreamingMetrics.streaming.lastReceivedBatch_processingEndTime")
      .getLong("value")
    val batchTime =endTime-startTime
    println(batchTime)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val btime=dateFormat.format(new Date(endTime))
    println(btime)
  }
}
