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
  protected int packWidth;
  /** data number in this pack. */
  protected int packNum;

  protected int rleGridVWidth;

  protected int rleGridCWidth;

  protected int rleGridSize;

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

    ArrayList<Integer> rleGridV;
    ArrayList<Integer> rleGridC;

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

    @Override
    public int readInt(ByteBuffer buffer) {
      int r = readT(buffer);
      return r;
    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int loadIntBatch(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridVWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridCWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridSize = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      // encodingLength = ceil(packNum * packWidth);
      encodingLength = ceil((rleGridVWidth + rleGridCWidth) * rleGridSize);
      diffBuf = new byte[encodingLength];
      buffer.get(diffBuf);
      allocateDataArray();

      rleGridV = new ArrayList<>();
      rleGridC = new ArrayList<>();
      for (int i = 0; i < rleGridSize; i++) {
        int rleGridV_c =
            BytesUtils.bytesToInt(diffBuf, (rleGridVWidth + rleGridCWidth) * i, rleGridVWidth);
        int rleGridC_c =
            BytesUtils.bytesToInt(
                diffBuf, (rleGridVWidth + rleGridCWidth) * i + rleGridVWidth, rleGridCWidth);
        rleGridV.add(rleGridV_c);
        rleGridC.add(rleGridC_c);
      }

      previous = firstValue;
      previousDiff = 0;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDiffBase = ReadWriteIOUtils.readInt(buffer);
      firstValue = ReadWriteIOUtils.readInt(buffer);
      grid = ReadWriteIOUtils.readInt(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new int[packNum];
    }

    @Override
    protected void readValue(int i) {
      int ii = i;
      int mark = 0;
      int v = 0;
      for (int j = 0; j < rleGridSize; j++) {
        if (ii >= rleGridC.get(j)) {
          ii = ii - rleGridC.get(j);
          mark += 1;
        } else {
          v = rleGridV.get(mark);
          break;
        }
      }
      // int v = BytesUtils.bytesToInt(diffBuf, packWidth * i, packWidth);

      // data[i] = previous + minDiffBase + v;
      data[i] = previous - previousDiff + grid + minDiffBase + v;
      previousDiff = minDiffBase + v;
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

    ArrayList<Long> rleGridV;
    ArrayList<Long> rleGridC;

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
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridVWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridCWidth = ReadWriteIOUtils.readInt(buffer);
      rleGridSize = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      // encodingLength = ceil(packNum * packWidth);
      encodingLength = ceil((rleGridVWidth + rleGridCWidth) * rleGridSize);
      diffBuf = new byte[encodingLength];
      buffer.get(diffBuf);
      allocateDataArray();

      rleGridV = new ArrayList<>();
      rleGridC = new ArrayList<>();
      for (int i = 0; i < rleGridSize; i++) {
        long rleGridV_c =
            BytesUtils.bytesToLong(diffBuf, (rleGridVWidth + rleGridCWidth) * i, rleGridVWidth);
        long rleGridC_c =
            BytesUtils.bytesToLong(
                diffBuf, (rleGridVWidth + rleGridCWidth) * i + rleGridVWidth, rleGridCWidth);
        rleGridV.add(rleGridV_c);
        rleGridC.add(rleGridC_c);
      }

      // for(int i=0;i<rleGridSize;i++)
      // {
      //  System.out.print(rleGridV.get(i));
      //  System.out.print(" ");
      //  System.out.println(rleGridC.get(i));
      // }

      previous = firstValue;
      previousDiff = 0;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < packNum; i++) {
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
    }

    @Override
    protected void allocateDataArray() {
      data = new long[packNum];
    }

    @Override
    protected void readValue(int i) {
      long ii = i;
      int mark = 0;
      long v = 0;
      for (int j = 0; j < rleGridSize; j++) {
        if (ii >= rleGridC.get(j)) {
          ii = ii - rleGridC.get(j);
          mark += 1;
        } else {
          v = rleGridV.get(mark);
          break;
        }
      }
      // long v = BytesUtils.bytesToLong(diffBuf, packWidth * i, packWidth);

      // data[i] = previous + minDiffBase + v;
      data[i] = previous - previousDiff + grid + minDiffBase + v;
      previousDiff = minDiffBase + v;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }
}
