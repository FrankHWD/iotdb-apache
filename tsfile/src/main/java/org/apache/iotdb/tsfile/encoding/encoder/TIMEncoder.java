/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

/**
 * TIMEncoder is a encoder for compressing data in type of integer and long. We adapt a hypothesis
 * that contiguous data points have similar values. Thus the difference value of two adjacent points
 * is smaller than those two point values. One integer in java takes 32-bits. If a positive number
 * is less than 2^m, the bits of this integer which index from m to 31 are all 0. Given an array
 * which length is n, if all values in input data array are all positive and less than 2^m, we need
 * actually m*n, but not 32*n bits to store the array.
 *
 * <p>TIMEncoder calculates difference between two adjacent points and record the minimum of those
 * difference values firstly. Then it saves two_diff value that difference minus minimum of them, to
 * make sure all two_diff values are positive. Then it statistics the longest bit length {@code m}
 * it takes for each two_diff value, which means the bit length that maximum two_diff value takes.
 * Only the low m bits are saved into result byte array for all two_diff values.
 */
public abstract class TIMEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE = 128;
  private static final Logger logger = LoggerFactory.getLogger(TIMEncoder.class);
  protected ByteArrayOutputStream out;
  protected int blockSize;
  // input value is stored in deltaBlackBuffer temporarily
  protected byte[] encodingBlockBuffer;

  protected int writeIndex = -1;
  protected int writeWidth = 0;

  /**
   * constructor of TIMEncoder.
   *
   * @param size - the number how many numbers to be packed into a block.
   */
  public TIMEncoder(int size) {
    super(TSEncoding.TIM);
    blockSize = size;
  }

  protected abstract void writeHeader() throws IOException;

  protected abstract void writeValueToBytes(int i);

  protected abstract void calcTwoDiff(int i);

  protected abstract void reset();

  protected abstract int calculateBitWidthsForDeltaBlockBuffer();

  /** write all data into {@code encodingBlockBuffer}. */
  private void writeDataWithMinWidth() {
    for (int i = 0; i < writeIndex; i++) {
      writeValueToBytes(i);
    }
    int encodingLength = (int) Math.ceil((double) (writeIndex * writeWidth) / 8.0);
    out.write(encodingBlockBuffer, 0, encodingLength);
  }

  private void writeHeaderToBytes() throws IOException {
    ReadWriteIOUtils.write(writeIndex, out);
    ReadWriteIOUtils.write(writeWidth, out);
    writeHeader();
  }

  private void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
    if (writeIndex == -1) {
      return;
    }
    // since we store the min delta, the deltas will be converted to be the
    // difference to min delta and all positive
    this.out = out;
    for (int i = 0; i < writeIndex; i++) {
      calcTwoDiff(i);
    }
    writeWidth = calculateBitWidthsForDeltaBlockBuffer();
    writeHeaderToBytes();
    writeDataWithMinWidth();

    reset();
    writeIndex = -1;
  }

  /** calling this method to flush all values which haven't encoded to result byte array. */
  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlockBuffer(out);
    } catch (IOException e) {
      logger.error("flush data to stream failed!", e);
    }
  }

  public static class IntTIMEncoder extends TIMEncoder {

    private int[] diffBlockBuffer;
    private int firstValue;
    private int previousValue;
    private int previousDiff;
    private int grid;
    private int minDiffBase;

    /** we save all value in a list and calculate its bitwidth. */
    protected Vector<Integer> values;

    protected ArrayList<Integer> diffs;

    public IntTIMEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of IntDeltaEncoder which is a sub-class of TIMEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public IntTIMEncoder(int size) {
      super(size);
      diffBlockBuffer = new int[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 4];
      values = new Vector<>();
      diffs = new ArrayList<>();
      reset();
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(diffBlockBuffer[i]));
      }
      return width;
    }

    private void calcDelta(int value) {
      int diff = -previousValue + previousDiff + value - grid; // calculate diff
      if (diff < minDiffBase) {
        minDiffBase = diff;
      }
      previousDiff = diff;
      diffBlockBuffer[writeIndex++] = diff;
    }

    /**
     * input a integer.
     *
     * @param value value to encode
     * @param out the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(int value, ByteArrayOutputStream out) {
      if (writeIndex == -1) {
        writeIndex++;
        firstValue = value;
        previousValue = firstValue;
        previousDiff = 0;
        grid = 0;
        values.add(value);
        return;
      }
      values.add(value);
      System.out.print(values.size());
      System.out.print("OK \n");
      writeIndex++;
      if (writeIndex == blockSize) {
        for (int i = 1; i <= blockSize; i++) {
          diffs.add(values.get(i) - values.get(i - 1));
        }
        Collections.sort(diffs);
        grid = diffs.get(blockSize / 2); // cal median

        writeIndex = 0;
        for (int i = 1; i <= blockSize; i++) {
          calcDelta(values.get(i));
          previousValue = values.get(i);
        }
        flush(out);
      }
    }

    @Override
    protected void reset() {
      firstValue = 0;
      previousValue = 0;
      previousDiff = 0;
      grid = 0;
      minDiffBase = Integer.MAX_VALUE;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        diffBlockBuffer[i] = 0;
      }
      values.clear();
      diffs.clear();
    }

    private int getValueWidth(int v) {
      return 32 - Integer.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes(int i) {
      BytesUtils.intToBytes(diffBlockBuffer[i], encodingBlockBuffer, writeWidth * i, writeWidth);
    }

    @Override
    protected void calcTwoDiff(int i) {
      diffBlockBuffer[i] = diffBlockBuffer[i] - minDiffBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(minDiffBase, out);
      ReadWriteIOUtils.write(firstValue, out);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return 4;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 24 is: index(4)+width(4)+minDiffBase(4)+firstValue(4)
      return (long) 24 + writeIndex * 4;
    }
  }

  public static class LongTIMEncoder extends TIMEncoder {

    private long[] diffBlockBuffer;
    private long firstValue;
    private long previousValue;
    private long previousDiff;
    private long grid;
    private long minDiffBase;

    /** we save all value in a list and calculate its bitwidth. */
    protected Vector<Long> values;

    protected ArrayList<Long> diffs;

    public LongTIMEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of LongDeltaEncoder which is a sub-class of TIMEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public LongTIMEncoder(int size) {
      super(size);
      diffBlockBuffer = new long[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 8];
      values = new Vector<>();
      diffs = new ArrayList<>();
      reset();
    }

    private void calcDelta(long value) {
      long diff = -previousValue + previousDiff + value - grid; // calculate diff
      if (diff < minDiffBase) {
        minDiffBase = diff;
      }
      previousDiff = diff;
      diffBlockBuffer[writeIndex++] = diff;
    }

    @Override
    protected void reset() {
      firstValue = 0L;
      previousValue = 0L;
      previousDiff = 0L;
      grid = 0;
      minDiffBase = Long.MAX_VALUE;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        diffBlockBuffer[i] = 0L;
      }
      values.clear();
      diffs.clear();
    }

    private int getValueWidth(long v) {
      return 64 - Long.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes(int i) {
      BytesUtils.longToBytes(diffBlockBuffer[i], encodingBlockBuffer, writeWidth * i, writeWidth);
    }

    @Override
    protected void calcTwoDiff(int i) {
      diffBlockBuffer[i] = diffBlockBuffer[i] - minDiffBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      out.write(BytesUtils.longToBytes(minDiffBase));
      out.write(BytesUtils.longToBytes(firstValue));
      out.write(BytesUtils.longToBytes(grid));
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return 8;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 24 is: index(4)+width(4)+minDiffBase(8)+firstValue(8)
      return (long) 24 + writeIndex * 8;
    }

    /**
     * input a integer or long value.
     *
     * @param value value to encode
     * @param out - the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(long value, ByteArrayOutputStream out) {
      if (writeIndex == -1) {
        writeIndex++;
        firstValue = value;
        previousValue = firstValue;
        previousDiff = 0;
        grid = 0;
        values.add(value);
        return;
      }
      values.add(value);
      writeIndex++;
      if (writeIndex == blockSize) {
        for (int i = 1; i <= blockSize; i++) {
          diffs.add(values.get(i) - values.get(i - 1));
        }
        Collections.sort(diffs);
        grid = diffs.get(blockSize / 2); // cal median

        writeIndex = 0;
        for (int i = 1; i <= blockSize; i++) {
          calcDelta(values.get(i));
          previousValue = values.get(i);
        }
        flush(out);
      }
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(diffBlockBuffer[i]));
      }
      return width;
    }
  }
}
