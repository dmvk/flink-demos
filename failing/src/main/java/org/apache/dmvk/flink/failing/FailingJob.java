package org.apache.dmvk.flink.failing;

import java.time.Duration;
import org.apache.dmvk.flink.common.RandomGeneratorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FailingJob {

  private static final int NUM_KEYS = 10;

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    final DataStreamSource<RandomGeneratorSource.RandomInput> input =
        env.fromSource(
            new RandomGeneratorSource(10),
            WatermarkStrategy.<RandomGeneratorSource.RandomInput>forBoundedOutOfOrderness(
                    Duration.ZERO)
                .withWatermarkAlignment("group", Duration.ofMinutes(1)),
            "random");

    input.keyBy(FailingJob::getKey).process(new TestFunction());

    env.execute();
  }

  private static String getKey(RandomGeneratorSource.RandomInput randomInput) {
    return "key-" + (randomInput.getNumber() % NUM_KEYS);
  }

  private static class TestFunction
      extends KeyedProcessFunction<String, RandomGeneratorSource.RandomInput, Void>
      implements CheckpointListener {

    @Override
    public void processElement(
        RandomGeneratorSource.RandomInput randomInput, Context context, Collector<Void> collector) {
      System.out.println(
          randomInput.getSplitIdx()
              + ": "
              + context.getCurrentKey()
              + "="
              + randomInput.getNumber());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      throw new Exception(String.format("Exception while competing checkpoint %d.", checkpointId));
    }
  }
}
