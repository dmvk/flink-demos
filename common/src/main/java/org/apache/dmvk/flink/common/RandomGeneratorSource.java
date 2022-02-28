package org.apache.dmvk.flink.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import lombok.Value;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

public class RandomGeneratorSource
    implements Source<
            RandomGeneratorSource.RandomInput,
            RandomGeneratorSource.RandomGeneratorSplit,
            Collection<RandomGeneratorSource.RandomGeneratorSplit>>,
        ResultTypeQueryable<RandomGeneratorSource.RandomInput> {

  private static final long serialVersionUID = 1L;

  public static Builder of() {
    return new Builder();
  }

  public static class Builder {

    private int numSplits = 1024;

    public Builder withNumSplits(int numSplits) {
      this.numSplits = numSplits;
      return this;
    }

    public RandomGeneratorSource build() {
      return new RandomGeneratorSource(numSplits);
    }
  }

  @Value
  public static class RandomInput {

    int splitIdx;
    int number;
  }

  @Value
  public static class RandomIterator implements Iterator<RandomInput> {

    int splitIdx;
    Random random;

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public RandomInput next() {
      return new RandomInput(splitIdx, random.nextInt());
    }
  }

  public static class RandomGeneratorSplit
      implements IteratorSourceSplit<RandomInput, RandomIterator> {

    private final int splitIdx;
    private final Random random;

    private RandomGeneratorSplit(int splitIdx) {
      this.splitIdx = splitIdx;
      this.random = new Random(splitIdx);
    }

    private RandomGeneratorSplit(int splitIdx, Random random) {
      this.splitIdx = splitIdx;
      this.random = random;
    }

    @Override
    public String splitId() {
      return "random-" + splitIdx;
    }

    @Override
    public RandomIterator getIterator() {
      return new RandomIterator(splitIdx, random);
    }

    @Override
    public IteratorSourceSplit<RandomInput, RandomIterator> getUpdatedSplitForIterator(
        RandomIterator iterator) {
      try {
        final Random clonedRandom = InstantiationUtil.clone(iterator.getRandom());
        return new RandomGeneratorSplit(splitIdx, clonedRandom);
      } catch (Exception e) {
        throw new IllegalStateException("Unable to clone the Random instance.", e);
      }
    }
  }

  private static class SplitSerializer implements SimpleVersionedSerializer<RandomGeneratorSplit> {

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(RandomGeneratorSplit split) throws IOException {
      DataOutputSerializer out = new DataOutputSerializer(1024);
      serializeV1(out, split);
      return out.getCopyOfBuffer();
    }

    @Override
    public RandomGeneratorSplit deserialize(int version, byte[] serialized) throws IOException {
      if (version != 1) {
        throw new IOException("Unrecognized version: " + version);
      }
      return deserializeV1(new DataInputDeserializer(serialized));
    }

    private static void serializeV1(DataOutputView out, RandomGeneratorSplit split)
        throws IOException {
      out.writeInt(split.splitIdx);
      final byte[] bytes = InstantiationUtil.serializeObject(split.random);
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    private static RandomGeneratorSplit deserializeV1(DataInputView in) throws IOException {
      final int splitIdx = in.readInt();
      final byte[] serializedRandom = new byte[in.readInt()];
      in.read(serializedRandom);
      try {
        final Random random =
            InstantiationUtil.deserializeObject(
                serializedRandom, ClassLoader.getSystemClassLoader());
        return new RandomGeneratorSplit(splitIdx, random);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }

  private static class CheckpointSerializer
      implements SimpleVersionedSerializer<Collection<RandomGeneratorSplit>> {

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(Collection<RandomGeneratorSplit> checkpoint) throws IOException {
      final DataOutputSerializer out = new DataOutputSerializer(1024);
      out.writeInt(checkpoint.size());
      for (RandomGeneratorSplit split : checkpoint) {
        SplitSerializer.serializeV1(out, split);
      }
      return out.getCopyOfBuffer();
    }

    @Override
    public Collection<RandomGeneratorSplit> deserialize(int version, byte[] serialized)
        throws IOException {
      if (version != 1) {
        throw new IOException("Unrecognized version: " + version);
      }
      final DataInputDeserializer in = new DataInputDeserializer(serialized);
      final int numSplits = in.readInt();
      final List<RandomGeneratorSplit> result = new ArrayList<>(numSplits);
      for (int remaining = numSplits; remaining > 0; --remaining) {
        result.add(SplitSerializer.deserializeV1(in));
      }
      return Collections.unmodifiableList(result);
    }
  }

  private final int numSplits;

  public RandomGeneratorSource(int numSplits) {
    this.numSplits = numSplits;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<RandomInput, RandomGeneratorSplit> createReader(
      SourceReaderContext readerContext) {
    return new IteratorSourceReader<>(readerContext);
  }

  @Override
  public SplitEnumerator<RandomGeneratorSplit, Collection<RandomGeneratorSplit>> createEnumerator(
      SplitEnumeratorContext<RandomGeneratorSplit> enumContext) {
    final List<RandomGeneratorSplit> splits = new ArrayList<>();
    for (int splitIdx = 0; splitIdx < numSplits; splitIdx++) {
      splits.add(new RandomGeneratorSplit(splitIdx));
    }
    return new IteratorSourceEnumerator<>(enumContext, Collections.unmodifiableList(splits));
  }

  @Override
  public SplitEnumerator<RandomGeneratorSplit, Collection<RandomGeneratorSplit>> restoreEnumerator(
      SplitEnumeratorContext<RandomGeneratorSplit> enumContext,
      Collection<RandomGeneratorSplit> checkpoint) {
    return new IteratorSourceEnumerator<>(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<RandomGeneratorSplit> getSplitSerializer() {
    return new SplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<Collection<RandomGeneratorSplit>>
      getEnumeratorCheckpointSerializer() {
    return new CheckpointSerializer();
  }

  @Override
  public TypeInformation<RandomInput> getProducedType() {
    return TypeInformation.of(RandomInput.class);
  }
}
