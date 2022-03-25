package org.apche.dmvk.backfill;

import java.io.IOException;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class RandomStatisticsJob {

  private static final long NUM_BUCKETS = 10_000;
  private static final long RANDOM_MIN = 0;
  private static final long RANDOM_MAX = 1_000_000;

  public static void main(String[] args) throws Exception {
    final Configuration configuration = new Configuration();
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    final TableDescriptor.Builder descriptorBuilder =
        TableDescriptor.forConnector("datagen")
            .schema(
                Schema.newBuilder()
                    .column("bucket", DataTypes.BIGINT())
                    .column("random", DataTypes.BIGINT())
                    .build())
            .option("fields.bucket.min", String.valueOf(0))
            .option("fields.bucket.max", String.valueOf(NUM_BUCKETS - 1))
            .option("fields.random.min", String.valueOf(RANDOM_MIN))
            .option("fields.random.max", String.valueOf(RANDOM_MAX));
    final boolean collectResults;
    switch (args[0]) {
      case "--bounded":
        collectResults = false;
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        descriptorBuilder
            .option(DataGenConnectorOptions.ROWS_PER_SECOND, Long.MAX_VALUE)
            .option(DataGenConnectorOptions.NUMBER_OF_ROWS, 10_000_000L);
        break;
      case "--unbounded":
        collectResults = true;
        descriptorBuilder.option(DataGenConnectorOptions.ROWS_PER_SECOND, 5L);
        break;
      default:
        throw new IllegalArgumentException("Invalid option.");
    }

    tableEnv
        .toDataStream(tableEnv.from(descriptorBuilder.build()), RandomNumberEvent.class)
        .keyBy(RandomStatisticsJob::getKey)
        .process(new CalculateStatistics(collectResults))
        .print();

    env.execute();
  }

  private static String getKey(RandomNumberEvent event) {
    return "bucket-" + event.getBucket();
  }

  public static class RandomNumberEvent {

    private final long bucket;
    private final long random;

    public RandomNumberEvent(long bucket, long random) {
      this.bucket = bucket;
      this.random = random;
    }

    public long getBucket() {
      return bucket;
    }

    public long getRandom() {
      return random;
    }
  }

  public static class Statistics {

    private final String bucket;
    private final long count;
    private final long min;
    private final long max;

    public Statistics(String bucket, long count, long min, long max) {
      this.bucket = bucket;
      this.count = count;
      this.min = min;
      this.max = max;
    }

    @Override
    public String toString() {
      return "ExampleOutput{"
          + "bucket='"
          + bucket
          + '\''
          + ", count="
          + count
          + ", min="
          + min
          + ", max="
          + max
          + '}';
    }
  }

  private static class CalculateStatistics
      extends KeyedProcessFunction<String, RandomNumberEvent, Statistics>
      implements CheckpointedFunction {

    private final boolean collectResults;

    private transient ValueState<Long> countState;
    private transient ValueState<Long> minState;
    private transient ValueState<Long> maxState;

    public CalculateStatistics(boolean collectResults) {
      this.collectResults = collectResults;
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
      countState =
          functionInitializationContext
              .getKeyedStateStore()
              .getState(new ValueStateDescriptor<>("count", Types.LONG));
      minState =
          functionInitializationContext
              .getKeyedStateStore()
              .getState(new ValueStateDescriptor<>("min", Types.LONG));
      maxState =
          functionInitializationContext
              .getKeyedStateStore()
              .getState(new ValueStateDescriptor<>("max", Types.LONG));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
      // No-op.
    }

    @Override
    public void processElement(
        RandomNumberEvent event, Context context, Collector<Statistics> collector)
        throws Exception {
      final long newCount = getOrDefault(countState, 1L) + 1L;
      countState.update(newCount);
      final long newMin = Math.min(getOrDefault(minState, Long.MAX_VALUE), event.getRandom());
      minState.update(newMin);
      final long newMax = Math.max(getOrDefault(minState, Long.MIN_VALUE), event.getRandom());
      maxState.update(newMax);
      if (collectResults) {
        collector.collect(new Statistics(context.getCurrentKey(), newCount, newMin, newMax));
      }
    }

    private static <T> T getOrDefault(ValueState<T> state, T defaultValue) throws IOException {
      final T value = state.value();
      if (value == null) {
        return defaultValue;
      }
      return value;
    }
  }
}
