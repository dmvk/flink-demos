package org.apache.dmvk.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class TestPipeline {

  public static void main(String[] args) {
    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    options.as(StreamingOptions.class).setStreaming(true);
    final Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(PeriodicImpulse.create().startAt(Instant.now()).withInterval(Duration.millis(10)))
        .apply(WithTimestamps.of(instant -> instant))
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                .via(
                    instant -> {
                      final String key = "key-" + instant.hashCode() % 10;
                      return KV.of(key, 1L);
                    }))
        .apply(Window.into(Sessions.withGapDuration(Duration.millis(50))))
        .apply(GroupByKey.create())
        .apply(Reshuffle.of())
        .apply(
            ParDo.of(
                new DoFn<KV<String, Iterable<Long>>, Void>() {

                  @ProcessElement
                  public void processElement(ProcessContext ctx) {
                    System.out.println("Output: " + ctx.element());
                  }
                }));

    pipeline.run();
  }
}
