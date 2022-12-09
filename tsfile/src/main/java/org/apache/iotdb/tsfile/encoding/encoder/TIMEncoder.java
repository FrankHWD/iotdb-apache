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

  // protected static final int BLOCK_DEFAULT_SIZE = 64;
  protected static final int BLOCK_DEFAULT_SIZE = 192;
  private static final Logger logger = LoggerFactory.getLogger(TIMEncoder.class);
  protected ByteArrayOutputStream out;
  protected int blockSize;
  // input value is stored in deltaBlackBuffer temporarily
  protected byte[] encodingBlockBuffer;

  protected ByteArrayOutputStream byteCache;

  protected int writeIndex = -1;
  // protected int writeWidth = 0;
  // protected int gridWidth = 0;
  protected int encodingLength = 0;
  protected int secondGDiffWidth = 0;
  protected int secondDDiffWidth = 0;
  protected int gridPosWidth = 0;
  protected int gridValWidth = 0;
  protected int gridArraySize = 0;
  protected int diffPosWidth = 0;
  protected int diffValWidth = 0;
  protected int diffArraySize = 0;
  protected boolean isAllOne = true;

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

  protected abstract void writeGValueToBytes(int i);

  protected abstract void writeDValueToBytes(int i);

  protected abstract void writeGridToBytes(int i);

  protected abstract void writeDiffToBytes(int i);

  protected abstract int calculateGridPosWidthsForDeltaBlockBuffer();

  protected abstract int calculateGridValWidthsForDeltaBlockBuffer();

  protected abstract int calculateDiffPosWidthsForDeltaBlockBuffer();

  protected abstract int calculateDiffValWidthsForDeltaBlockBuffer();

  // protected abstract void calcTwoDiff(int i);

  protected abstract long calcMinMax();

  protected abstract void reset();

  protected abstract int calculateSecondGDiffWidthsForDeltaBlockBuffer();

  protected abstract int calculateSecondDDiffWidthsForDeltaBlockBuffer();

  protected abstract void processDiff();

  /** write all data into {@code encodingBlockBuffer}. */
  private void writeDataWithMinWidth() {
    if (!isAllOne) {
      if (secondGDiffWidth != 0) {
        for (int i = 0; i < writeIndex - 1; i++) {
          writeGValueToBytes(i);
        }
      }
      for (int i = 0; i < gridArraySize; i++) {
        writeGridToBytes(i);
      }
    }
    encodingLength =
        (int)
            Math.ceil(
                (double)
                        ((writeIndex - 1) * secondGDiffWidth
                            + (writeIndex - 1) * secondDDiffWidth
                            + gridArraySize * (gridPosWidth + gridValWidth)
                            + diffArraySize * (diffPosWidth + diffValWidth))
                    / 8.0);
    if (secondDDiffWidth != 0) {
      for (int i = 0; i < writeIndex - 1; i++) {
        writeDValueToBytes(i);
      }
    }
    if (diffArraySize != 0) {
      for (int i = 0; i < diffArraySize; i++) {
        writeDiffToBytes(i);
      }
    }
    out.write(encodingBlockBuffer, 0, encodingLength);
  }

  private void writeHeaderToBytes() throws IOException {
    ReadWriteIOUtils.write(writeIndex, out);
    // ReadWriteIOUtils.write(writeWidth, out);
    ReadWriteIOUtils.write(secondGDiffWidth, out);
    ReadWriteIOUtils.write(secondDDiffWidth, out);
    ReadWriteIOUtils.write(gridPosWidth, out);
    ReadWriteIOUtils.write(gridValWidth, out);
    ReadWriteIOUtils.write(gridArraySize, out);
    ReadWriteIOUtils.write(diffPosWidth, out);
    ReadWriteIOUtils.write(diffValWidth, out);
    ReadWriteIOUtils.write(diffArraySize, out);
    writeHeader();
  }

  private void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
    if (writeIndex == -1) {
      return;
    }

    if (writeIndex < blockSize) {
      processDiff();
    }

    // since we store the min delta, the deltas will be converted to be the
    // difference to min delta and all positive
    this.out = out;
    // for (int i = 0; i < writeIndex; i++) {
    //  calcTwoDiff(i);
    // }
    // writeWidth = calculateBitWidthsForDeltaBlockBuffer();
    // gridWidth = calculateGridWidthsForDeltaBlockBuffer();
    secondGDiffWidth = calculateSecondGDiffWidthsForDeltaBlockBuffer();
    secondDDiffWidth = calculateSecondDDiffWidthsForDeltaBlockBuffer();
    gridPosWidth = calculateGridPosWidthsForDeltaBlockBuffer();
    gridValWidth = calculateGridValWidthsForDeltaBlockBuffer();
    diffPosWidth = calculateDiffPosWidthsForDeltaBlockBuffer();
    diffValWidth = calculateDiffValWidthsForDeltaBlockBuffer();

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
    private int[] gridNumBuffer;
    private int firstValue;
    private int firstGValue2;
    private int firstDValue2;
    private int previousValue;
    private int previousDiff;
    private int grid;
    private int minDiffBase;
    private int maxDiffBase;
    private int minGDiffBase2;
    private int minDDiffBase2;

    /** we save all value in a list and calculate its bitwidth. */
    protected Vector<Integer> values;

    protected ArrayList<Integer> diffs;

    ArrayList<Integer> secondGDiffs;
    ArrayList<Integer> secondDDiffs;

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
      gridNumBuffer = new int[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 4];
      values = new Vector<>();
      diffs = new ArrayList<>();
      secondGDiffs = new ArrayList<>();
      secondDDiffs = new ArrayList<>();
      reset();
    }

    @Override
    protected void reset() {
      firstValue = 0;
      firstGValue2 = 0;
      firstDValue2 = 0;
      previousValue = 0;
      previousDiff = 0;
      grid = 0;
      minDiffBase = Integer.MAX_VALUE;
      maxDiffBase = Integer.MIN_VALUE;
      minGDiffBase2 = Integer.MAX_VALUE;
      minDDiffBase2 = Integer.MAX_VALUE;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        diffBlockBuffer[i] = 0;
        gridNumBuffer[i] = 0;
      }
      values.clear();
      diffs.clear();
      secondGDiffs.clear();
      secondDDiffs.clear();
    }

    private int getValueWidth(int v) {
      return 32 - Integer.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeGValueToBytes(int i) {
      BytesUtils.intToBytes(
          secondGDiffs.get(i), encodingBlockBuffer, secondGDiffWidth * i, secondGDiffWidth);
    }

    @Override
    protected void writeGridToBytes(int i) {}

    @Override
    protected void writeDiffToBytes(int i) {}

    @Override
    protected void writeDValueToBytes(int i) {
      BytesUtils.intToBytes(
          secondDDiffs.get(i),
          encodingBlockBuffer,
          (writeIndex - 1) * secondGDiffWidth + secondDDiffWidth * i,
          secondDDiffWidth);
    }

    // @Override
    // protected void calcTwoDiff(int i) {
    //  diffBlockBuffer[i] = diffBlockBuffer[i] - minDiffBase;
    // }

    protected long calcMinMax() {
      return maxDiffBase - minDiffBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(minDiffBase, out);
      ReadWriteIOUtils.write(firstValue, out);
      ReadWriteIOUtils.write(grid, out);
      ReadWriteIOUtils.write(minGDiffBase2, out);
      ReadWriteIOUtils.write(firstGValue2, out);
      ReadWriteIOUtils.write(minDDiffBase2, out);
      ReadWriteIOUtils.write(firstDValue2, out);
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
      writeIndex++;
      if (writeIndex == blockSize) {
        processDiff();
        flush(out);
      }
    }

    protected void processDiff() {
      int dSize = writeIndex;
      for (int i = 1; i <= dSize; i++) {
        diffs.add(values.get(i) - values.get(i - 1));
      }
      Collections.sort(diffs);
      grid = diffs.get(dSize / 2); // cal median

      writeIndex = 0;
      for (int i = 1; i <= dSize; i++) {
        calcDelta(values.get(i));
        previousValue = values.get(i);
      }

      for (int i = 1; i < dSize; i++) {
        int secondGDiff = gridNumBuffer[i] - gridNumBuffer[i - 1];
        if (secondGDiff < minGDiffBase2) {
          minGDiffBase2 = secondGDiff;
        }
      }
      firstGValue2 = gridNumBuffer[0];
      for (int i = 1; i < dSize; i++) {
        int secondGDiff = gridNumBuffer[i] - gridNumBuffer[i - 1];
        secondGDiffs.add(secondGDiff - minGDiffBase2);
      }

      for (int i = 1; i < dSize; i++) {
        int secondDDiff = diffBlockBuffer[i] - diffBlockBuffer[i - 1];
        if (secondDDiff < minDDiffBase2) {
          minDDiffBase2 = secondDDiff;
        }
      }
      firstDValue2 = diffBlockBuffer[0] - minDiffBase;
      for (int i = 1; i < dSize; i++) {
        int secondDDiff = diffBlockBuffer[i] - diffBlockBuffer[i - 1];
        secondDDiffs.add(secondDDiff - minDDiffBase2);
      }
    }

    private void calcDelta(int value) {
      // long diff = -previousValue + previousDiff + value - grid; // calculate diff

      int gridNum = (int) Math.round((value - previousValue + previousDiff) * 1.0 / grid);
      // int gridNum = (int) ((value - previousValue + previousDiff)/grid);
      int diff = -previousValue + previousDiff + value - gridNum * grid; // calculate diff
      if (diff < minDiffBase) {
        minDiffBase = diff;
      }
      if (diff > maxDiffBase) {
        maxDiffBase = diff;
      }
      previousDiff = diff;
      gridNumBuffer[writeIndex] = gridNum;
      diffBlockBuffer[writeIndex++] = diff;
    }

    @Override
    protected int calculateSecondGDiffWidthsForDeltaBlockBuffer() {
      int secondGDiffWidth = 0;
      for (int i = 0; i < writeIndex - 1; i++) {
        secondGDiffWidth = Math.max(secondGDiffWidth, getValueWidth(secondGDiffs.get(i)));
      }
      return secondGDiffWidth;
    }

    @Override
    protected int calculateSecondDDiffWidthsForDeltaBlockBuffer() {
      int secondDDiffWidth = 0;
      for (int i = 0; i < writeIndex - 1; i++) {
        secondDDiffWidth = Math.max(secondDDiffWidth, getValueWidth(secondDDiffs.get(i)));
      }
      return secondDDiffWidth;
    }

    @Override
    protected int calculateGridPosWidthsForDeltaBlockBuffer() {
      int gridWidth = 0;
      return gridWidth;
    }

    @Override
    protected int calculateGridValWidthsForDeltaBlockBuffer() {
      int gridWidth = 0;
      return gridWidth;
    }

    @Override
    protected int calculateDiffPosWidthsForDeltaBlockBuffer() {
      int gridWidth = 0;
      return gridWidth;
    }

    @Override
    protected int calculateDiffValWidthsForDeltaBlockBuffer() {
      int gridWidth = 0;
      return gridWidth;
    }
  }

  public static class LongTIMEncoder extends TIMEncoder {

    private long[] diffBlockBuffer;
    private long[] gridNumBuffer;
    private long firstValue;
    private long previousValue;
    private long previousDiff;
    private long grid;
    private long minDiffBase;
    private long maxDiffBase;
    private long minGDiffBase2;
    private long firstGValue2;
    private long minDDiffBase2;
    private long firstDValue2;
    private long minDDiffBase3;

    ArrayList<Long> secondGDiffs;
    ArrayList<Long> secondDDiffs;

    protected ArrayList<Long> gridPosArray;
    protected ArrayList<Long> gridValArray;

    protected ArrayList<Long> diffPosArray;
    protected ArrayList<Long> diffValArray;

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
      gridNumBuffer = new long[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 8];
      values = new Vector<>();
      diffs = new ArrayList<>();
      secondGDiffs = new ArrayList<>();
      secondDDiffs = new ArrayList<>();
      gridPosArray = new ArrayList<>();
      gridValArray = new ArrayList<>();
      diffPosArray = new ArrayList<>();
      diffValArray = new ArrayList<>();
      reset();
    }

    @Override
    protected void reset() {
      firstValue = 0L;
      firstGValue2 = 0L;
      firstDValue2 = 0L;
      previousValue = 0L;
      previousDiff = 0L;
      grid = 0;
      isAllOne = true;
      gridArraySize = 0;
      diffArraySize = 0;
      minDiffBase = Long.MAX_VALUE;
      minGDiffBase2 = Long.MAX_VALUE;
      minDDiffBase2 = Long.MAX_VALUE;
      minDDiffBase3 = Long.MAX_VALUE;
      maxDiffBase = Long.MIN_VALUE;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        diffBlockBuffer[i] = 0L;
        gridNumBuffer[i] = 0L;
      }
      values.clear();
      diffs.clear();
      secondGDiffs.clear();
      secondDDiffs.clear();
      gridPosArray.clear();
      gridValArray.clear();
      diffPosArray.clear();
      diffValArray.clear();
    }

    private int getValueWidth(long v) {
      return 64 - Long.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeGValueToBytes(int i) {
      BytesUtils.longToBytes(
          secondGDiffs.get(i), encodingBlockBuffer, secondGDiffWidth * i, secondGDiffWidth);
    }

    @Override
    protected void writeDValueToBytes(int i) {
      BytesUtils.longToBytes(
          secondDDiffs.get(i),
          encodingBlockBuffer,
          secondGDiffWidth * (writeIndex - 1) + secondDDiffWidth * i,
          secondDDiffWidth);
    }

    @Override
    protected void writeGridToBytes(int i) {
      BytesUtils.longToBytes(
          gridPosArray.get(i),
          encodingBlockBuffer,
          (writeIndex - 1) * secondGDiffWidth
              + (writeIndex - 1) * secondDDiffWidth
              + (gridPosWidth + gridValWidth) * i,
          gridPosWidth);
      BytesUtils.longToBytes(
          gridValArray.get(i),
          encodingBlockBuffer,
          (writeIndex - 1) * secondGDiffWidth
              + (writeIndex - 1) * secondDDiffWidth
              + (gridPosWidth + gridValWidth) * i
              + gridPosWidth,
          gridValWidth);
    }

    @Override
    protected void writeDiffToBytes(int i) {
      BytesUtils.longToBytes(
          diffPosArray.get(i),
          encodingBlockBuffer,
          (writeIndex - 1) * secondGDiffWidth
              + (writeIndex - 1) * secondDDiffWidth
              + (gridPosWidth + gridValWidth) * gridArraySize
              + (diffPosWidth + diffValWidth) * i,
          diffPosWidth);
      BytesUtils.longToBytes(
          diffValArray.get(i),
          encodingBlockBuffer,
          (writeIndex - 1) * secondGDiffWidth
              + (writeIndex - 1) * secondDDiffWidth
              + (gridPosWidth + gridValWidth) * gridArraySize
              + (diffPosWidth + diffValWidth) * i
              + diffPosWidth,
          diffValWidth);
    }

    // @Override
    // protected void calcTwoDiff(int i) {
    //  diffBlockBuffer[i] = diffBlockBuffer[i] - minDiffBase;
    // }

    protected long calcMinMax() {
      return maxDiffBase - minDiffBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      if (isAllOne) {
        minGDiffBase2 = 0;
      }
      out.write(BytesUtils.longToBytes(minDiffBase));
      out.write(BytesUtils.longToBytes(firstValue));
      out.write(BytesUtils.longToBytes(grid));
      out.write(BytesUtils.longToBytes(minGDiffBase2));
      out.write(BytesUtils.longToBytes(firstGValue2));
      out.write(BytesUtils.longToBytes(minDDiffBase2));
      out.write(BytesUtils.longToBytes(firstDValue2));
      if (diffArraySize == 0) {
        minDDiffBase3 = 0;
      }
      out.write(BytesUtils.longToBytes(minDDiffBase3));
      // out.write(BytesUtils.intToBytes(gridWidth));
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
      // The meaning of 24+4 is: index(4)+width(4)+minDiffBase(8)+firstValue(8)+grid(8)
      return (long) 32 + writeIndex * 8;
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
        processDiff();
        flush(out);
      }
    }

    protected void processDiff() {
      int dSize = writeIndex;
      for (int i = 1; i <= dSize; i++) {
        diffs.add(values.get(i) - values.get(i - 1));
      }
      Collections.sort(diffs);
      grid = diffs.get(dSize / 2); // cal median

      writeIndex = 0;
      for (int i = 1; i <= dSize; i++) {
        calcDelta(values.get(i));
        previousValue = values.get(i);
      }

      isAllOne = true;
      for (int i = 0; i < dSize; i++) {
        if (gridNumBuffer[i] != 1) {
          isAllOne = false;
          break;
        }
      }

      if (!isAllOne) {
        int last_i = 0;
        for (int i = 1; i < dSize; i++) {
          if (Math.abs(gridNumBuffer[i] - gridNumBuffer[i - 1]) > 3) {
            gridPosArray.add((long) i - last_i);
            last_i = i;
            gridValArray.add(gridNumBuffer[i] - gridNumBuffer[i - 1]);
            gridNumBuffer[i] = 0;
          }
        }
        gridArraySize = gridPosArray.size();

        for (int i = 1; i < dSize; i++) {
          long secondGDiff = gridNumBuffer[i] - gridNumBuffer[i - 1];
          if (secondGDiff < minGDiffBase2) {
            minGDiffBase2 = secondGDiff;
          }
        }
        firstGValue2 = gridNumBuffer[0];
        for (int i = 1; i < dSize; i++) {
          long secondGDiff = gridNumBuffer[i] - gridNumBuffer[i - 1];
          secondGDiffs.add(secondGDiff - minGDiffBase2);
        }
      }

      for (int i = 1; i < dSize; i++) {
        long secondDDiff = diffBlockBuffer[i] - diffBlockBuffer[i - 1];
        if (secondDDiff < minDDiffBase2) {
          minDDiffBase2 = secondDDiff;
        }
      }
      firstDValue2 = diffBlockBuffer[0] - minDiffBase;
      for (int i = 1; i < dSize; i++) {
        long secondDDiff = diffBlockBuffer[i] - diffBlockBuffer[i - 1];
        secondDDiffs.add(secondDDiff - minDDiffBase2);
      }

      ArrayList<Long> secondDDiffs_med = new ArrayList<>(secondDDiffs);
      Collections.sort(secondDDiffs_med);
      Long med = secondDDiffs_med.get(dSize / 2);
      int count_out = 0;
      for (int i = 0; i < dSize - 1; i++) {
        if (secondDDiffs.get(i) > med + 31 || secondDDiffs.get(i) < med - 31) {
          count_out += 1;
        } else {
          if (secondDDiffs.get(i) < minDDiffBase3) {
            minDDiffBase3 = secondDDiffs.get(i);
          }
        }
      }
      if (count_out < 25) {
        for (int i = 0; i < dSize - 1; i++) {
          if (secondDDiffs.get(i) > med + 31 || secondDDiffs.get(i) < med - 31) {
            diffPosArray.add((long) i);
            diffValArray.add(secondDDiffs.get(i));
            secondDDiffs.set(i, med - minDDiffBase3);
          } else {
            long tmp = secondDDiffs.get(i);
            secondDDiffs.set(i, tmp - minDDiffBase3);
          }
        }
        diffArraySize = diffPosArray.size();
      }
    }

    private void calcDelta(long value) {
      // long diff = -previousValue + previousDiff + value - grid; // calculate diff

      int gridNum = (int) Math.round((value - previousValue + previousDiff) * 1.0 / grid);
      // int gridNum = (int) ((value - previousValue + previousDiff)/grid);
      long diff = -previousValue + previousDiff + value - gridNum * grid; // calculate diff
      if (diff < minDiffBase) {
        minDiffBase = diff;
      }
      if (diff > maxDiffBase) {
        maxDiffBase = diff;
      }
      previousDiff = diff;
      gridNumBuffer[writeIndex] = gridNum;
      diffBlockBuffer[writeIndex++] = diff;
    }

    @Override
    protected int calculateSecondGDiffWidthsForDeltaBlockBuffer() {
      int secondGDiffWidth = 0;
      if (isAllOne) return secondGDiffWidth;
      for (int i = 0; i < writeIndex - 1; i++) {
        secondGDiffWidth = Math.max(secondGDiffWidth, getValueWidth(secondGDiffs.get(i)));
      }
      return secondGDiffWidth;
    }

    @Override
    protected int calculateSecondDDiffWidthsForDeltaBlockBuffer() {
      int secondDDiffWidth = 0;
      for (int i = 0; i < writeIndex - 1; i++) {
        secondDDiffWidth = Math.max(secondDDiffWidth, getValueWidth(secondDDiffs.get(i)));
      }
      return secondDDiffWidth;
    }

    @Override
    protected int calculateGridPosWidthsForDeltaBlockBuffer() {
      int gridPosWidth = 0;
      if (isAllOne) return gridPosWidth;
      for (int i = 0; i < gridArraySize; i++) {
        gridPosWidth = Math.max(gridPosWidth, getValueWidth(gridPosArray.get(i)));
      }
      return gridPosWidth;
    }

    @Override
    protected int calculateGridValWidthsForDeltaBlockBuffer() {
      int gridValWidth = 0;
      if (isAllOne) return gridValWidth;
      for (int i = 0; i < gridArraySize; i++) {
        gridValWidth = Math.max(gridValWidth, getValueWidth(gridValArray.get(i)));
      }
      return gridValWidth;
    }

    @Override
    protected int calculateDiffPosWidthsForDeltaBlockBuffer() {
      int diffPosWidth = 0;
      for (int i = 0; i < diffArraySize; i++) {
        diffPosWidth = Math.max(diffPosWidth, getValueWidth(diffPosArray.get(i)));
      }
      return diffPosWidth;
    }

    @Override
    protected int calculateDiffValWidthsForDeltaBlockBuffer() {
      int diffValWidth = 0;
      for (int i = 0; i < diffArraySize; i++) {
        diffValWidth = Math.max(diffValWidth, getValueWidth(diffValArray.get(i)));
      }
      return diffValWidth;
    }
  }
}
