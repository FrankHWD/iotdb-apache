package org.apache.iotdb.tsfile.encoding;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TextLocationRaw {
  public static int getBitWith(int num) {
    return 32 - Integer.numberOfLeadingZeros(num);
  }

  public static byte[] int2Bytes(int integer) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (integer >> 24);
    bytes[1] = (byte) (integer >> 16);
    bytes[2] = (byte) (integer >> 8);
    bytes[3] = (byte) integer;
    return bytes;
  }

  public static byte[] double2Bytes(double dou) {
    long value = Double.doubleToRawLongBits(dou);
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) ((value >> 8 * i) & 0xff);
    }
    return bytes;
  }

  public static double bytes2Double(ArrayList<Byte> encoded, int start, int num) {
    if (num > 8) {
      System.out.println("bytes2Doubleerror");
      return 0;
    }
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (encoded.get(i + start) & 0xff)) << (8 * i);
    }
    return Double.longBitsToDouble(value);
  }

  public static byte[] float2bytes(float f) {
    int fbit = Float.floatToIntBits(f);
    byte[] b = new byte[4];
    for (int i = 0; i < 4; i++) {
      b[i] = (byte) (fbit >> (24 - i * 8));
    }
    int len = b.length;
    byte[] dest = new byte[len];
    System.arraycopy(b, 0, dest, 0, len);
    byte temp;
    for (int i = 0; i < len / 2; ++i) {
      temp = dest[i];
      dest[i] = dest[len - i - 1];
      dest[len - i - 1] = temp;
    }
    return dest;
  }

  public static float bytes2float(ArrayList<Byte> b, int index) {
    int l;
    l = b.get(index);
    l &= 0xff;
    l |= ((long) b.get(index + 1) << 8);
    l &= 0xffff;
    l |= ((long) b.get(index + 2) << 16);
    l &= 0xffffff;
    l |= ((long) b.get(index + 3) << 24);
    return Float.intBitsToFloat(l);
  }

  public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
    int value = 0;
    if (num > 4) {
      System.out.println("bytes2Integer error");
      return 0;
    }
    for (int i = 0; i < num; i++) {
      value <<= 8;
      int b = encoded.get(i + start) & 0xFF;
      value |= b;
    }
    return value;
  }

  public static byte[] bitPacking(ArrayList<Integer> numbers, int bit_width) {
    int block_num = numbers.size() / 8;
    byte[] result = new byte[bit_width * block_num];
    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(i * 8 + k) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int index, int bit_width) {
    int block_num = (numbers.size() - 1) / 8;
    byte[] result = new byte[bit_width * block_num];
    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(i * 8 + k + 1).get(index) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static byte[] bitPacking1(ArrayList<Integer> numbers, int bit_width) {
    int block_num = numbers.size() / 8;
    byte[] result = new byte[bit_width * block_num];
    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(i * 8 + k) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static byte[] bitPacking2(
      ArrayList<ArrayList<Integer>> numbers, int index, int bit_width) {
    int block_num = numbers.size() / 8;
    byte[] result = new byte[bit_width * block_num];
    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(i * 8 + k).get(index) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static ArrayList<Integer> decodebitPacking(
      ArrayList<Byte> encoded, int decode_pos, int bit_width, int min_delta, int block_size) {
    ArrayList<Integer> result_list = new ArrayList<>();
    for (int i = 0; i < (block_size - 1) / 8; i++) { // bitpacking  纵向8个，bit width是多少列
      int[] val8 = new int[8];
      for (int j = 0; j < 8; j++) {
        val8[j] = 0;
      }
      for (int j = 0; j < bit_width; j++) {
        byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
        byte[] bit8 = new byte[8];
        for (int k = 0; k < 8; k++) {
          bit8[k] = (byte) (tmp_byte & 1);
          tmp_byte = (byte) (tmp_byte >> 1);
        }
        for (int k = 0; k < 8; k++) {
          val8[k] = val8[k] * 2 + bit8[k];
        }
      }
      for (int j = 0; j < 8; j++) {
        result_list.add(val8[j] + min_delta);
      }
      decode_pos += bit_width;
    }
    return result_list;
  }

  public static ArrayList<Integer> decodebitPacking2(
      ArrayList<Byte> encoded, int decode_pos, int bit_width, int min_delta, int block_size) {
    ArrayList<Integer> result_list = new ArrayList<>();
    for (int i = 0; i < block_size / 8; i++) { // bitpacking  纵向8个，bit width是多少列
      int[] val8 = new int[8];
      for (int j = 0; j < 8; j++) {
        val8[j] = 0;
      }
      for (int j = 0; j < bit_width; j++) {
        byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
        byte[] bit8 = new byte[8];
        for (int k = 0; k < 8; k++) {
          bit8[k] = (byte) (tmp_byte & 1);
          tmp_byte = (byte) (tmp_byte >> 1);
        }
        for (int k = 0; k < 8; k++) {
          val8[k] = val8[k] * 2 + bit8[k];
        }
      }
      for (int j = 0; j < 8; j++) {
        result_list.add(val8[j] + min_delta);
      }
      decode_pos += bit_width;
    }
    return result_list;
  }

  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(
      ArrayList<ArrayList<Integer>> ts_block,
      int block_size,
      int grid,
      ArrayList<Integer> result,
      ArrayList<Integer> gridnum_block,
      ArrayList<Integer> gridpos) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    int pre_gridNum_r = 0;
    for (int j = 1; j < block_size; j++) {
      int gridNum_r =
          (int) Math.round((ts_block.get(j).get(0) - ts_block.get(0).get(0)) * 1.0 / grid);
      int epsilon_r = ts_block.get(j).get(0) - ts_block.get(0).get(0) - gridNum_r * grid;
      int epsilon_v = ts_block.get(j).get(1) - ts_block.get(j - 1).get(1);
      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      tmp.add(gridNum_r - pre_gridNum_r);
      ts_block_delta.add(tmp);

      pre_gridNum_r = gridNum_r;
    }

    int max_gridnum_val = Integer.MIN_VALUE;
    for (int j = 1; j < block_size; j++) {
      if (ts_block_delta.get(j).get(2) != 1) {
        gridnum_block.add(ts_block_delta.get(j).get(2));
        gridpos.add(1);
        if (ts_block_delta.get(j).get(2) > max_gridnum_val) {
          max_gridnum_val = ts_block_delta.get(j).get(2);
        }
      } else {
        gridpos.add(0);
      }
    }
    int timestamp_gridnum_length = gridnum_block.size();
    int timestamp_gridnum_remain_length;
    if (timestamp_gridnum_length % 8 == 0) {
      timestamp_gridnum_remain_length = 0;
    } else {
      timestamp_gridnum_remain_length = 8 - timestamp_gridnum_length % 8;
    }
    for (int j = 0; j < timestamp_gridnum_remain_length; j++) {
      gridpos.add(0);
      gridnum_block.add(0);
    }

    int max_interval = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    for (int j = block_size - 1; j > 0; j--) {
      int epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
      int epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
      if (epsilon_r > max_interval) {
        max_interval = epsilon_r;
      }
      if (epsilon_v > max_value) {
        max_value = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.set(j, tmp);
    }

    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);
    int max_bit_width_gridnum_val = getBitWith(max_gridnum_val);

    int length =
        (max_bit_width_interval + max_bit_width_value) * (block_size - 1)
            + (max_bit_width_gridnum_val + 1)
                * (timestamp_gridnum_length + timestamp_gridnum_remain_length);
    result.clear();

    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);
    result.add(max_bit_width_gridnum_val);

    result.add(timestamp_delta_min);
    result.add(value_delta_min);
    result.add(timestamp_gridnum_length);

    return ts_block_delta;
  }

  public static ArrayList<Byte> encode2Bytes(
      ArrayList<ArrayList<Integer>> ts_block,
      ArrayList<Integer> raw_length,
      ArrayList<Integer> gridnum_block,
      ArrayList<Integer> gridpos) {

    ArrayList<Byte> encoded_result = new ArrayList<>();

    byte[] flag_byte = int2Bytes(1);
    for (byte b : flag_byte) encoded_result.add(b);

    // encode interval0 and value0
    byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

    // encode interval_min and value_min
    byte[] interval_min_byte = int2Bytes(raw_length.get(4));
    for (byte b : interval_min_byte) encoded_result.add(b);
    byte[] value_min_byte = int2Bytes(raw_length.get(5));
    for (byte b : value_min_byte) encoded_result.add(b);

    // encode interval
    byte[] max_bit_width_interval_byte = int2Bytes(raw_length.get(1));
    for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
    byte[] timestamp_bytes = bitPacking(ts_block, 0, raw_length.get(1));
    for (byte b : timestamp_bytes) encoded_result.add(b);

    // encode value
    byte[] max_bit_width_value_byte = int2Bytes(raw_length.get(2));
    for (byte b : max_bit_width_value_byte) encoded_result.add(b);
    byte[] value_bytes = bitPacking(ts_block, 1, raw_length.get(2));
    for (byte b : value_bytes) encoded_result.add(b);

    // encode gridnum_value
    byte[] max_bit_width_gridnum_value_byte = int2Bytes(raw_length.get(3));
    for (byte b : max_bit_width_gridnum_value_byte) encoded_result.add(b);
    byte[] gridnum_val_bytes = bitPacking1(gridnum_block, raw_length.get(3));
    for (byte b : gridnum_val_bytes) encoded_result.add(b);

    byte[] gridnum_pos_bytes = bitPacking1(gridpos, 1);
    for (byte b : gridnum_pos_bytes) encoded_result.add(b);

    return encoded_result;
  }

  public static ArrayList<Byte> ReorderingRegressionEncoder(
      ArrayList<ArrayList<Integer>> data, int block_size) {
    block_size++;
    ArrayList<Byte> encoded_result = new ArrayList<>();
    int length_all = data.size();
    byte[] length_all_bytes = int2Bytes(length_all);
    for (byte b : length_all_bytes) encoded_result.add(b);
    int block_num = length_all / block_size;

    byte[] block_size_byte = int2Bytes(block_size);
    for (byte b : block_size_byte) encoded_result.add(b);

    for (int i = 0; i < block_num; i++) {
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
      for (int j = 0; j < block_size; j++) {
        ts_block.add(data.get(j + i * block_size));
      }

      ArrayList<Integer> raw_length = new ArrayList<>(); // parameters
      ArrayList<Integer> gridnum_block = new ArrayList<>();
      ArrayList<Integer> gridpos = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block_delta =
          getEncodeBitsRegression(ts_block, block_size, 1, raw_length, gridnum_block, gridpos);

      ArrayList<Byte> cur_encoded_result;
      cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, gridnum_block, gridpos);
      encoded_result.addAll(cur_encoded_result);
    }
    return encoded_result;
  }

  public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded) {
    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
    int decode_pos = 0;
    int length_all = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;
    int block_size = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;

    int block_num = length_all / block_size;

    for (int k = 0; k < block_num; k++) {
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();
      ArrayList<Integer> gridnum_pos_list = new ArrayList<>();
      ArrayList<Integer> gridnum_val_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int flag = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      if (flag == 1) {
        int time0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int time_min = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int value_min = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int timestamp_gridnum_length = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int timestamp_gridnum_remain_length;
        if (timestamp_gridnum_length % 8 == 0) {
          timestamp_gridnum_remain_length = 0;
        } else {
          timestamp_gridnum_remain_length = 8 - timestamp_gridnum_length % 8;
        }
        int grid_length = timestamp_gridnum_length + timestamp_gridnum_remain_length;

        int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        time_list = decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, block_size);
        decode_pos += max_bit_width_time * (block_size - 1) / 8;

        int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list = decodebitPacking(encoded, decode_pos, max_bit_width_value, 0, block_size);
        decode_pos += max_bit_width_value * (block_size - 1) / 8;

        int max_bit_width_gridnum_pos = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        gridnum_pos_list =
            decodebitPacking2(encoded, decode_pos, max_bit_width_gridnum_pos, 0, grid_length);
        decode_pos += max_bit_width_gridnum_pos * grid_length / 8;

        int max_bit_width_gridnum_val = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        gridnum_val_list =
            decodebitPacking2(encoded, decode_pos, max_bit_width_gridnum_val, 0, grid_length);
        decode_pos += max_bit_width_gridnum_val * grid_length / 8;

        int grid = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int vi_pre = value0;
        int gridnum_pre = 0;
        int gridnum_pos = 0;
        int g = 0;
        for (int i = 0; i < block_size - 1; i++) {
          int gridnum = 1;
          if (g < timestamp_gridnum_length && (gridnum_pos + gridnum_pos_list.get(g)) == i) {
            gridnum = gridnum_val_list.get(g);
            gridnum_pos = gridnum_pos + gridnum_pos_list.get(g);
            g += 1;
          }
          int ti = time0 + (gridnum + gridnum_pre) * grid + time_list.get(i) + time_min;
          time_list.set(i, ti);
          gridnum_pre = gridnum + gridnum_pre;

          int vi = vi_pre + value_list.get(i) + value_min;
          value_list.set(i, vi);
          vi_pre = vi;
        }

        ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
        ts_block_tmp0.add(time0);
        ts_block_tmp0.add(value0);
        ts_block.add(ts_block_tmp0);
        for (int i = 0; i < block_size - 1; i++) {
          ArrayList<Integer> ts_block_tmp = new ArrayList<>();
          ts_block_tmp.add(time_list.get(i));
          ts_block_tmp.add(value_list.get(i));
          ts_block.add(ts_block_tmp);
        }
        data.addAll(ts_block);
      }
    }
    return data;
  }

  public static void main(String[] args) throws IOException {
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<Integer> dataset_block_size = new ArrayList<>();

    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Metro-Traffic");
    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
                    + "\\compression_ratio\\rr_ratio\\Metro-Traffic_ratio.csv");

    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

      String inputPath = input_path_list.get(file_i);
      String Output = output_path_list.get(file_i);

      int repeatTime = 1;

      File file = new File(inputPath);
      File[] tempList = file.listFiles();

      CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

      String[] head = {
        "Input Direction",
        "Encoding Algorithm",
        "Encoding Time",
        "Decoding Time",
        "Points",
        "Compressed Size",
        "Compression Ratio"
      };
      writer.writeRecord(head);

      assert tempList != null;

      for (File f : tempList) {
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();
        ArrayList<ArrayList<Integer>> data_decoded = new ArrayList<>();

        loader.readHeaders();
        data.clear();
        while (loader.readRecord()) {
          ArrayList<Integer> tmp = new ArrayList<>();
          String s = loader.getValues()[0];
          tmp.add(Integer.valueOf(s));
          tmp.add(Integer.valueOf(loader.getValues()[1]));
          data.add(tmp);
        }
        inputStream.close();
        long encodeTime = 0;
        long decodeTime = 0;
        double ratio = 0;
        double compressed_size = 0;
        int repeatTime2 = 1;

        CompressionType comp = CompressionType.UNCOMPRESSED;
        // CompressionType comp = CompressionType.LZ4;
        // CompressionType comp = CompressionType.GZIP;
        ICompressor compressor = ICompressor.getCompressor(comp);
        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

        for (int i = 0; i < repeatTime; i++) {
          long s = System.nanoTime();
          ArrayList<Byte> buffer = new ArrayList<>();
          for (int repeat = 0; repeat < repeatTime2; repeat++) {
            buffer = ReorderingRegressionEncoder(data, 256);
          }
          long e = System.nanoTime();
          encodeTime += ((e - s) / repeatTime2);

          byte[] elems = new byte[buffer.size()];
          for (int b = 0; b < buffer.size(); b++) {
            elems[i] = buffer.get(i);
          }
          byte[] compressed = compressor.compress(elems);
          compressed_size += compressed.length;
          double ratioTmp = (double) compressed.length / (double) (data.size() * Integer.BYTES * 2);

          // compressed_size += buffer.size();
          // double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);

          ratio += ratioTmp;
          s = System.nanoTime();
          for (int repeat = 0; repeat < repeatTime2; repeat++) {
            // data_decoded = ReorderingRegressionDecoder(buffer);
          }
          e = System.nanoTime();
          decodeTime += ((e - s) / repeatTime2);

          //          for(int j=0;j<data_decoded.size();j++){
          //              System.out.print(j);
          //              System.out.print(" ");
          //              System.out.print(data.get(j).get(0));
          //              System.out.print(" ");
          //              System.out.println(data_decoded.get(j).get(0));
          //          }

        }

        ratio /= repeatTime;
        compressed_size /= repeatTime;
        encodeTime /= repeatTime;
        decodeTime /= repeatTime;

        String[] record = {
          f.toString(),
          "TIM",
          String.valueOf(encodeTime),
          String.valueOf(decodeTime),
          String.valueOf(data.size()),
          String.valueOf(compressed_size),
          String.valueOf(ratio)
        };
        System.out.println(ratio);
        writer.writeRecord(record);
      }
      writer.close();
    }
  }
}
