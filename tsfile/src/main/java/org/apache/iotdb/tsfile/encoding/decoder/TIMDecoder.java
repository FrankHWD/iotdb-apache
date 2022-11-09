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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.TIMEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * This class is a decoder for decoding the byte array that encoded by {@code TIMEncoder}.TIMDecoder
 * just supports integer and long values.<br>
 * .
 *
 * @see TIMEncoder
 */
public abstract class TIMDecoder extends Decoder {

  protected long count = 0;
  protected byte[] diffBuf;

  /** the first value in one pack. */
  protected int readIntTotalCount = 0;

  protected int nextReadIndex = 0;
  /** max bit length of all value in a pack. */
  // protected int writeWidth;
  /** data number in this pack. */
  protected int writeIndex;

  protected int rleGridVWidth;

  protected int rleGridCWidth;

  protected int rleGridSize;

  protected int rleWriteVWidth;

  protected int rleWriteCWidth;

  protected int rleWriteSize;

  // protected int girdWidth;

  /** how many bytes data takes after encoding. */
  protected int encodingLength;

  public TIMDecoder() {
    super(TSEncoding.TIM);
  }

  protected abstract void readHeader(ByteBuffer buffer) throws IOException;

  protected abstract void allocateDataArray();

  protected abstract void readValue(int i);

  /**
   * calculate the bytes length containing v bits.
   *
   * @param v - number of bits
   * @return number of bytes
   */
  protected int ceil(int v) {
    return (int) Math.ceil((double) (v) / 8.0);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < readIntTotalCount) || buffer.remaining() > 0;
  }

  public static class IntTIMDecoder extends TIMDecoder {

    private int firstValue;
    private int[] data;
    private int previous;
    private int previousDiff;
    /** minimum value for all difference. */
    private int minDiffBase;

    private int grid;

    private int gridWidth;

    ArrayList<Integer> rleGridV;
    ArrayList<Integer> rleGridC;
    ArrayList<Integer> rleWriteV;
    ArrayList<Integer> rleWriteC;

    public IntTIMDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        return loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int loadIntBatch(ByteBuffer buffer) {
      writeIndex = ReadWriteIOUtils.readInt(buffer);
      // writeWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridVWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridCWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridSize = ReadWriteIOUtils.readInt(buffer);
      rleWriteVWidth = ReadWriteIOUtils.readInt(buffer);
      rleWriteCWidth = ReadWriteIOUtils.readInt(buffer);
      rleWriteSize = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      // encodingLength = ceil(writeIndex * writeWidth);
      encodingLength =
          ceil(
              (rleWriteVWidth + rleWriteCWidth) * rleWriteSize
                  + (rleGridVWidth + rleGridCWidth) * rleGridSize);
      diffBuf = new byte[encodingLength];
      buffer.get(diffBuf);
      allocateDataArray();

      rleGridV = new ArrayList<>();
      rleGridC = new ArrayList<>();
      rleWriteV = new ArrayList<>();
      rleWriteC = new ArrayList<>();
      for (int i = 0; i < rleGridSize; i++) {
        int rleGridV_c =
            BytesUtils.bytesToInt(
                diffBuf,
                (rleWriteVWidth + rleWriteCWidth) * rleWriteSize
                    + (rleGridVWidth + rleGridCWidth) * i,
                rleGridVWidth);
        int rleGridC_c =
            BytesUtils.bytesToInt(
                diffBuf,
                (rleWriteVWidth + rleWriteCWidth) * rleWriteSize
                    + (rleGridVWidth + rleGridCWidth) * i
                    + rleGridVWidth,
                rleGridCWidth);
        rleGridV.add(rleGridV_c);
        rleGridC.add(rleGridC_c);
      }

      for (int i = 0; i < rleWriteSize; i++) {
        int rleWriteV_c =
            BytesUtils.bytesToInt(diffBuf, (rleWriteVWidth + rleWriteCWidth) * i, rleWriteVWidth);
        int rleWriteC_c =
            BytesUtils.bytesToInt(
                diffBuf, (rleWriteVWidth + rleWriteCWidth) * i + rleWriteVWidth, rleWriteCWidth);
        rleWriteV.add(rleWriteV_c);
        rleWriteC.add(rleWriteC_c);
      }

      previous = firstValue;
      previousDiff = 0;
      readIntTotalCount = writeIndex;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < writeIndex; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      int r = readT(buffer);
      return r;
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDiffBase = ReadWriteIOUtils.readInt(buffer);
      firstValue = ReadWriteIOUtils.readInt(buffer);
      grid = ReadWriteIOUtils.readInt(buffer);
      // gridWidth = ReadWriteIOUtils.readInt(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new int[writeIndex];
    }

    @Override
    protected void readValue(int i) {
      // long v = BytesUtils.bytesToLong(diffBuf, writeWidth * i, writeWidth);
      // data[i] = previous + minDiffBase + v;
      // data[i] = previous - previousDiff + grid + minDiffBase + v;
      // previousDiff = minDiffBase + v;

      int ii = i;
      int mark = 0;
      int gridNum = 0;
      for (int j = 0; j < rleGridSize; j++) {
        if (ii >= rleGridC.get(j)) {
          ii = ii - rleGridC.get(j);
          mark += 1;
        } else {
          gridNum = rleGridV.get(mark);
          break;
        }
      }

      int ii2 = i;
      int mark2 = 0;
      int v2 = 0;
      for (int j = 0; j < rleWriteSize; j++) {
        if (ii2 >= rleWriteC.get(j)) {
          ii2 = ii2 - rleWriteC.get(j);
          mark2 += 1;
        } else {
          v2 = rleWriteV.get(mark2);
          break;
        }
      }

      // int v = BytesUtils.bytesToInt(diffBuf, (writeWidth) * i, writeWidth);
      data[i] = previous - previousDiff + grid * gridNum + minDiffBase + v2;
      previousDiff = minDiffBase + v2;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }

  public static class LongTIMDecoder extends TIMDecoder {

    private long firstValue;
    private long[] data;
    private long previous;
    private long previousDiff;
    /** minimum value for all difference. */
    private long minDiffBase;

    private long grid;

    private int gridWidth;

    ArrayList<Long> rleGridV;
    ArrayList<Long> rleGridC;
    ArrayList<Long> rleWriteV;
    ArrayList<Long> rleWriteC;

    public LongTIMDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    protected long readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        return loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    protected long loadIntBatch(ByteBuffer buffer) {
      writeIndex = ReadWriteIOUtils.readInt(buffer);
      // writeWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridVWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridCWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridSize = ReadWriteIOUtils.readInt(buffer);
      rleWriteVWidth = ReadWriteIOUtils.readInt(buffer);
      rleWriteCWidth = ReadWriteIOUtils.readInt(buffer);
      rleWriteSize = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      // encodingLength = ceil(writeIndex * writeWidth);
      encodingLength =
          ceil(
              (rleWriteVWidth + rleWriteCWidth) * rleWriteSize
                  + (rleGridVWidth + rleGridCWidth) * rleGridSize);
      diffBuf = new byte[encodingLength];
      buffer.get(diffBuf);
      allocateDataArray();

      rleGridV = new ArrayList<>();
      rleGridC = new ArrayList<>();
      rleWriteV = new ArrayList<>();
      rleWriteC = new ArrayList<>();

      if (rleGridSize > 1) {
        for (int i = 0; i < rleGridSize; i++) {
          long rleGridV_c =
              BytesUtils.bytesToLong(
                  diffBuf,
                  (rleWriteVWidth + rleWriteCWidth) * rleWriteSize
                      + (rleGridVWidth + rleGridCWidth) * i,
                  rleGridVWidth);
          rleGridV.add(rleGridV_c);
        }
        for (int i = 0; i < rleGridSize; i++) {
          long rleGridC_c =
              BytesUtils.bytesToLong(
                  diffBuf,
                  (rleWriteVWidth + rleWriteCWidth) * rleWriteSize
                      + (rleGridVWidth + rleGridCWidth) * i
                      + rleGridVWidth,
                  rleGridCWidth);
          rleGridC.add(rleGridC_c);
        }
      } else {
        rleGridV.add(1L);
        rleGridC.add((long) writeIndex);
      }

      for (int i = 0; i < rleWriteSize; i++) {
        long rleWriteV_c =
            BytesUtils.bytesToLong(diffBuf, (rleWriteVWidth + rleWriteCWidth) * i, rleWriteVWidth);
        long rleWriteC_c =
            BytesUtils.bytesToLong(
                diffBuf, (rleWriteVWidth + rleWriteCWidth) * i + rleWriteVWidth, rleWriteCWidth);
        rleWriteV.add(rleWriteV_c);
        rleWriteC.add(rleWriteC_c);
      }

      previous = firstValue;
      previousDiff = 0;
      readIntTotalCount = writeIndex;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < writeIndex; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    public long readLong(ByteBuffer buffer) {
      long r = readT(buffer);
      return r;
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDiffBase = ReadWriteIOUtils.readLong(buffer);
      firstValue = ReadWriteIOUtils.readLong(buffer);
      grid = ReadWriteIOUtils.readLong(buffer);
      // gridWidth = ReadWriteIOUtils.readInt(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new long[writeIndex];
    }

    @Override
    protected void readValue(int i) {
      // long v = BytesUtils.bytesToLong(diffBuf, writeWidth * i, writeWidth);
      // data[i] = previous + minDiffBase + v;
      // data[i] = previous - previousDiff + grid + minDiffBase + v;
      // previousDiff = minDiffBase + v;

      long ii = i;
      int mark = 0;
      long gridNum = 0;
      for (int j = 0; j < rleGridSize; j++) {
        if (ii >= rleGridC.get(j)) {
          ii = ii - rleGridC.get(j);
          mark += 1;
        } else {
          gridNum = rleGridV.get(mark);
          break;
        }
      }

      long ii2 = i;
      int mark2 = 0;
      long v2 = 0;
      for (int j = 0; j < rleWriteSize; j++) {
        if (ii2 >= rleWriteC.get(j)) {
          ii2 = ii2 - rleWriteC.get(j);
          mark2 += 1;
        } else {
          v2 = rleWriteV.get(mark2);
          break;
        }
      }

      // long v = BytesUtils.bytesToLong(diffBuf, (writeWidth) * i, writeWidth);
      // long gridNum = BytesUtils.bytesToLong(diffBuf, (writeWidth) * i + writeWidth - gridWidth,
      // gridWidth);
      data[i] = previous - previousDiff + grid * gridNum + minDiffBase + v2;
      previousDiff = minDiffBase + v2;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }
}
