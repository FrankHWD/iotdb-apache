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

  protected int secondGDiffWidth;
  protected int secondDDiffWidth;

  protected int gridPosWidth;
  protected int gridValWidth;
  protected int gridArraySize;

  protected int diffPosWidth;
  protected int diffValWidth;
  protected int diffArraySize;

  // protected int girdWidth;

  /** how many bytes data takes after encoding. */
  protected int encodingLength = 0;

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
    private int firstGValue2;
    private int firstDValue2;
    private int[] data;
    private int previous;
    private int previousDiff;
    private int prevGV;
    private int prevDV;
    /** minimum value for all difference. */
    private int minDiffBase;

    private int minGDiffBase2;
    private int minDDiffBase2;

    private int grid;

    // private int gridWidth;

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
      secondGDiffWidth = ReadWriteIOUtils.readInt(buffer);
      secondDDiffWidth = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      encodingLength =
          ceil((writeIndex - 1) * secondGDiffWidth + (writeIndex - 1) * secondDDiffWidth);

      if (encodingLength == 0) {
        for (int i = 0; i < writeIndex; i++) {
          data[i] = 0;
        }
        return 0;
      }

      diffBuf = new byte[encodingLength];
      buffer.get(diffBuf);
      allocateDataArray();

      previous = firstValue;
      previousDiff = 0;
      prevGV = 0;
      prevDV = 0;
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
      minGDiffBase2 = ReadWriteIOUtils.readInt(buffer);
      firstGValue2 = ReadWriteIOUtils.readInt(buffer);
      minDDiffBase2 = ReadWriteIOUtils.readInt(buffer);
      firstDValue2 = ReadWriteIOUtils.readInt(buffer);
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

      int gridNum;
      if (i == 0) {
        gridNum = firstGValue2;
      } else {
        if (secondGDiffWidth == 0) {
          gridNum = prevGV + minGDiffBase2;
        } else {
          int gridNum_c =
              BytesUtils.bytesToInt(diffBuf, secondGDiffWidth * (i - 1), secondGDiffWidth);
          gridNum = prevGV + gridNum_c + minGDiffBase2;
        }
      }
      prevGV = gridNum;

      int v2;
      if (i == 0) {
        v2 = firstDValue2;
      } else {
        if (secondDDiffWidth == 0) {
          v2 = prevDV + minDDiffBase2;
        } else {
          int v2_c =
              BytesUtils.bytesToInt(
                  diffBuf,
                  (writeIndex - 1) * secondGDiffWidth + secondDDiffWidth * (i - 1),
                  secondDDiffWidth);
          v2 = prevDV + v2_c + minDDiffBase2;
        }
      }
      prevDV = v2;

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
    // private long firstGValue2;
    private long firstDValue2;
    private long[] data;
    private long previous;
    private long previousDiff;
    private long prevGV;
    private long prevDV;
    /** minimum value for all difference. */
    private long minDiffBase;

    // private long minGDiffBase2;
    private long minDDiffBase2;

    private long minDDiffBase3;

    private long grid;

    ArrayList<Long> gridPosArray;
    ArrayList<Long> gridValArray;

    ArrayList<Long> diffPosArray;
    ArrayList<Long> diffValArray;

    // private int gridWidth;

    private int pos = 0;

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
      // secondGDiffWidth = ReadWriteIOUtils.readInt(buffer);
      secondDDiffWidth = ReadWriteIOUtils.readInt(buffer);
      gridPosWidth = ReadWriteIOUtils.readInt(buffer);
      gridValWidth = ReadWriteIOUtils.readInt(buffer);
      gridArraySize = ReadWriteIOUtils.readInt(buffer);
      diffPosWidth = ReadWriteIOUtils.readInt(buffer);
      diffValWidth = ReadWriteIOUtils.readInt(buffer);
      diffArraySize = ReadWriteIOUtils.readInt(buffer);

      count++;
      readHeader(buffer);

      encodingLength =
          ceil(
              // (writeIndex - 1) * secondGDiffWidth
              (writeIndex - 1) * secondDDiffWidth
                  + gridArraySize * (gridPosWidth + gridValWidth)
                  + diffArraySize * (diffPosWidth + diffValWidth));

      gridPosArray = new ArrayList<>();
      gridValArray = new ArrayList<>();

      diffPosArray = new ArrayList<>();
      diffValArray = new ArrayList<>();

      if (encodingLength == 0) {
        for (int i = 0; i < writeIndex; i++) {
          data[i] = 0;
        }
        return 0;
      }

      diffBuf = new byte[encodingLength];
      buffer.get(diffBuf);
      allocateDataArray();

      long last_i = 0;
      for (int i = 0; i < gridArraySize; i++) {
        long gridPos =
            BytesUtils.bytesToLong(
                diffBuf,
                // (writeIndex - 1) * secondGDiffWidth
                (writeIndex - 1) * secondDDiffWidth + (gridPosWidth + gridValWidth) * i,
                gridPosWidth);
        long gridVal =
            BytesUtils.bytesToLong(
                diffBuf,
                // (writeIndex - 1) * secondGDiffWidth
                (writeIndex - 1) * secondDDiffWidth
                    + (gridPosWidth + gridValWidth) * i
                    + gridPosWidth,
                gridValWidth);
        gridPosArray.add(gridPos + last_i);
        last_i = last_i + gridPos;
        gridValArray.add(gridVal);
      }

      long las_i = 0;
      for (int i = 0; i < diffArraySize; i++) {
        long diffPos =
            BytesUtils.bytesToLong(
                diffBuf,
                // (writeIndex - 1) * secondGDiffWidth
                (writeIndex - 1) * secondDDiffWidth
                    + (gridPosWidth + gridValWidth) * gridArraySize
                    + (diffPosWidth + diffValWidth) * i,
                diffPosWidth);
        long diffVal =
            BytesUtils.bytesToLong(
                diffBuf,
                // (writeIndex - 1) * secondGDiffWidth
                (writeIndex - 1) * secondDDiffWidth
                    + (gridPosWidth + gridValWidth) * gridArraySize
                    + (diffPosWidth + diffValWidth) * i
                    + diffPosWidth,
                diffValWidth);
        diffPosArray.add(diffPos + las_i);
        las_i = las_i + diffPos;
        diffValArray.add(diffVal);
      }

      previous = firstValue;
      previousDiff = 0;
      readIntTotalCount = writeIndex;
      nextReadIndex = 0;
      prevGV = 0;
      prevDV = 0;
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
      // minGDiffBase2 = ReadWriteIOUtils.readLong(buffer);
      // firstGValue2 = ReadWriteIOUtils.readLong(buffer);
      minDDiffBase2 = ReadWriteIOUtils.readLong(buffer);
      firstDValue2 = ReadWriteIOUtils.readLong(buffer);
      minDDiffBase3 = ReadWriteIOUtils.readLong(buffer);
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

      //      long gridNum;
      //      long gridNum2;
      //      if (secondGDiffWidth != 0) {
      //        if (i == 0) {
      //          gridNum = firstGValue2;
      //          gridNum2 = gridNum;
      //        } else {
      //          long gridNum_c =
      //              BytesUtils.bytesToLong(diffBuf, secondGDiffWidth * (i - 1), secondGDiffWidth);
      //          gridNum = prevGV + gridNum_c + minGDiffBase2;
      //          gridNum2 = gridNum;
      //          for (int j = 0; j < gridArraySize; j++) {
      //            if ((long) i == gridPosArray.get(j)) {
      //              gridNum = prevGV + gridValArray.get(j);
      //              break;
      //            }
      //          }
      //        }
      //        prevGV = gridNum2;
      //      } else {
      //        gridNum = 1;
      //      }

      long gridNum = 1;
      if (gridArraySize != 0) {
        for (int j = 0; j < gridArraySize; j++) {
          if ((long) i == gridPosArray.get(j)) {
            gridNum = gridValArray.get(j);
            break;
          }
        }
      }

      //      long gridNum = 1;
      //      if (gridArraySize != 0) {
      //        long gridNum_c = BytesUtils.bytesToLong(diffBuf, i, 1);
      //        if(gridNum_c==0){
      //          gridNum=gridValArray.get(pos);
      //          pos+=1;
      //        }
      //      }

      long v2;
      if (i == 0) {
        v2 = firstDValue2;
      } else {
        if (secondDDiffWidth == 0) {
          v2 = prevDV + minDDiffBase2;
        } else {
          long v2_c =
              BytesUtils.bytesToLong(
                      diffBuf,
                      // secondGDiffWidth * (writeIndex - 1) +
                      secondDDiffWidth * (i - 1),
                      secondDDiffWidth)
                  + minDDiffBase3;
          for (int j = 0; j < diffArraySize; j++) {
            if ((long) i - 1 == diffPosArray.get(j)) {
              v2_c = diffValArray.get(j);
              break;
            }
          }
          v2 = prevDV + v2_c + minDDiffBase2;
        }
      }
      prevDV = v2;

      // long v = BytesUtils.bytesToLong(diffBuf, (writeWidth) * i, writeWidth);
      // long gridNum =
      //    BytesUtils.bytesToLong(diffBuf, (writeWidth) * i + writeWidth - gridWidth, gridWidth);
      data[i] = previous - previousDiff + grid * gridNum + minDiffBase + v2;
      previousDiff = minDiffBase + v2;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }
}
