package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class TestMultipleGridNumRaw {
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
    int block_num = numbers.size() / 8;
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

  public static void quickSort(
      ArrayList<ArrayList<Integer>> ts_block, int index, int low, int high) {
    if (low >= high) return;
    ArrayList<Integer> pivot = ts_block.get(low);
    int l = low;
    int r = high;
    ArrayList<Integer> temp;
    while (l < r) {
      while (l < r && ts_block.get(r).get(index) >= pivot.get(index)) {
        r--;
      }
      while (l < r && ts_block.get(l).get(index) <= pivot.get(index)) {
        l++;
      }
      if (l < r) {
        temp = ts_block.get(l);
        ts_block.set(l, ts_block.get(r));
        ts_block.set(r, temp);
      }
    }
    ts_block.set(low, ts_block.get(l));
    ts_block.set(l, pivot);
    if (low < l) {
      quickSort(ts_block, index, low, l - 1);
    }
    if (r < high) {
      quickSort(ts_block, index, r + 1, high);
    }
  }

  public static int getCommon(int m, int n) {
    int z;
    while (m % n != 0) {
      z = m % n;
      m = n;
      n = z;
    }
    return n;
  }

  public static void splitTimeStamp3(
      ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> result) {
    int td_common = 0;
    for (int i = 1; i < ts_block.size(); i++) {
      int time_diffi = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
      if (td_common == 0) {
        if (time_diffi != 0) {
          td_common = time_diffi;
          continue;
        } else {
          continue;
        }
      }
      if (time_diffi != 0) {
        td_common = getCommon(time_diffi, td_common);
        if (td_common == 1) {
          break;
        }
      }
    }
    if (td_common == 0) {
      td_common = 1;
    }

    int t0 = ts_block.get(0).get(0);
    for (int i = 0; i < ts_block.size(); i++) {
      ArrayList<Integer> tmp = new ArrayList<>();
      int interval_i = (ts_block.get(i).get(0) - t0) / td_common;
      tmp.add(t0 + interval_i);
      tmp.add(ts_block.get(i).get(1));
      ts_block.set(i, tmp);
    }
    result.add(td_common);
  }

  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(
      ArrayList<ArrayList<Integer>> ts_block,
      int block_size,
      ArrayList<Integer> result,
      ArrayList<Integer> i_star,
      ArrayList<Float> theta) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    theta.clear();

    long sum_X_r = 0;
    long sum_Y_r = 0;
    long sum_squ_X_r = 0;
    long sum_squ_XY_r = 0;
    long sum_X_v = 0;
    long sum_Y_v = 0;
    long sum_squ_X_v = 0;
    long sum_squ_XY_v = 0;

    for (int i = 1; i < block_size; i++) {
      sum_X_r += ts_block.get(i - 1).get(0);
      sum_X_v += ts_block.get(i - 1).get(1);
      sum_Y_r += ts_block.get(i).get(0);
      sum_Y_v += ts_block.get(i).get(1);
      sum_squ_X_r += ((long) ts_block.get(i - 1).get(0) * ts_block.get(i - 1).get(0));
      sum_squ_X_v += ((long) ts_block.get(i - 1).get(1) * ts_block.get(i - 1).get(1));
      sum_squ_XY_r += ((long) ts_block.get(i - 1).get(0) * ts_block.get(i).get(0));
      sum_squ_XY_v += ((long) ts_block.get(i - 1).get(1) * ts_block.get(i).get(1));
    }

    int m_reg = block_size - 1;
    float theta0_r = 0.0F;
    float theta1_r = 1.0F;
    if (m_reg * sum_squ_X_r != sum_X_r * sum_X_r) {
      theta0_r =
          (float) (sum_squ_X_r * sum_Y_r - sum_X_r * sum_squ_XY_r)
              / (float) (m_reg * sum_squ_X_r - sum_X_r * sum_X_r);
      theta1_r =
          (float) (m_reg * sum_squ_XY_r - sum_X_r * sum_Y_r)
              / (float) (m_reg * sum_squ_X_r - sum_X_r * sum_X_r);
    }

    float theta0_v = 0.0F;
    float theta1_v = 1.0F;
    if (m_reg * sum_squ_X_v != sum_X_v * sum_X_v) {
      theta0_v =
          (float) (sum_squ_X_v * sum_Y_v - sum_X_v * sum_squ_XY_v)
              / (float) (m_reg * sum_squ_X_v - sum_X_v * sum_X_v);
      theta1_v =
          (float) (m_reg * sum_squ_XY_v - sum_X_v * sum_Y_v)
              / (float) (m_reg * sum_squ_X_v - sum_X_v * sum_X_v);
    }

    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    // delta to Regression
    for (int j = 1; j < block_size; j++) {
      int epsilon_r =
          ts_block.get(j).get(0)
              - (int) (theta0_r + theta1_r * (double) ts_block.get(j - 1).get(0));
      int epsilon_v =
          ts_block.get(j).get(1)
              - (int) (theta0_v + theta1_v * (double) ts_block.get(j - 1).get(1));

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.add(tmp);
    }

    int max_interval = Integer.MIN_VALUE;
    int max_interval_i = -1;
    int max_value = Integer.MIN_VALUE;
    int max_value_i = -1;
    for (int j = block_size - 1; j > 0; j--) {
      int epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
      int epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
      if (epsilon_r > max_interval) {
        max_interval = epsilon_r;
        max_interval_i = j;
      }
      if (epsilon_v > max_value) {
        max_value = epsilon_v;
        max_value_i = j;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.set(j, tmp);
    }

    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);

    // calculate error
    int length = (max_bit_width_interval + max_bit_width_value) * (block_size - 1);
    result.clear();

    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);

    result.add(timestamp_delta_min);
    result.add(value_delta_min);

    theta.add(theta0_r);
    theta.add(theta1_r);
    theta.add(theta0_v);
    theta.add(theta1_v);

    i_star.add(max_interval_i);
    i_star.add(max_value_i);

    return ts_block_delta;
  }

  public static ArrayList<Byte> encode2Bytes(
      ArrayList<ArrayList<Integer>> ts_block,
      ArrayList<Integer> raw_length,
      ArrayList<Float> theta,
      ArrayList<Integer> result2) {
    ArrayList<Byte> encoded_result = new ArrayList<>();

    // encode interval0 and value0
    byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

    // encode theta
    byte[] theta0_r_byte = float2bytes(theta.get(0) + raw_length.get(3));
    for (byte b : theta0_r_byte) encoded_result.add(b);
    byte[] theta1_r_byte = float2bytes(theta.get(1));
    for (byte b : theta1_r_byte) encoded_result.add(b);
    byte[] theta0_v_byte = float2bytes(theta.get(2) + raw_length.get(4));
    for (byte b : theta0_v_byte) encoded_result.add(b);
    byte[] theta1_v_byte = float2bytes(theta.get(3));
    for (byte b : theta1_v_byte) encoded_result.add(b);

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

    byte[] td_common_byte = int2Bytes(result2.get(0));
    for (byte b : td_common_byte) encoded_result.add(b);

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

    // encode block size (Integer)
    byte[] block_size_byte = int2Bytes(block_size);
    for (byte b : block_size_byte) encoded_result.add(b);

    for (int i = 0; i < block_num; i++) {
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
      for (int j = 0; j < block_size; j++) {
        ts_block.add(data.get(j + i * block_size));
      }

      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block, result2);

      quickSort(ts_block, 0, 0, block_size - 1);

      // time-order
      ArrayList<Integer> raw_length =
          new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<Float> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta =
          getEncodeBitsRegression(ts_block, block_size, raw_length, i_star_ready, theta);

      ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, theta, result2);
      encoded_result.addAll(cur_encoded_result);
    }

    int remaining_length = length_all - block_num * block_size;
    if (remaining_length == 1) {
      byte[] timestamp_end_bytes = int2Bytes(data.get(data.size() - 1).get(0));
      for (byte b : timestamp_end_bytes) encoded_result.add(b);
      byte[] value_end_bytes = int2Bytes(data.get(data.size() - 1).get(1));
      for (byte b : value_end_bytes) encoded_result.add(b);
    }
    if (remaining_length != 0 && remaining_length != 1) {
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      for (int j = block_num * block_size; j < length_all; j++) {
        ts_block.add(data.get(j));
      }
      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block, result2);

      quickSort(ts_block, 0, 0, remaining_length - 1);

      ArrayList<Integer> raw_length =
          new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<Float> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta =
          getEncodeBitsRegression(ts_block, remaining_length, raw_length, i_star_ready, theta);

      int supple_length;
      if (remaining_length % 8 == 0) {
        supple_length = 1;
      } else if (remaining_length % 8 == 1) {
        supple_length = 0;
      } else {
        supple_length = 9 - remaining_length % 8;
      }
      for (int s = 0; s < supple_length; s++) {
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(0);
        tmp.add(0);
        ts_block_delta.add(tmp);
      }
      ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, theta, result2);
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
    int remain_length = length_all - block_num * block_size;
    int zero_number;
    if (remain_length % 8 == 0) {
      zero_number = 1;
    } else if (remain_length % 8 == 1) {
      zero_number = 0;
    } else {
      zero_number = 9 - remain_length % 8;
    }

    for (int k = 0; k < block_num; k++) {
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      float theta0_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta0_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list = decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, block_size);
      decode_pos += max_bit_width_time * (block_size - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list = decodebitPacking(encoded, decode_pos, max_bit_width_value, 0, block_size);
      decode_pos += max_bit_width_value * (block_size - 1) / 8;

      int td_common = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int ti_pre = time0;
      int vi_pre = value0;
      for (int i = 0; i < block_size - 1; i++) {
        int ti = (int) ((double) theta1_r * ti_pre + (double) theta0_r + time_list.get(i));
        time_list.set(i, ti);
        ti_pre = ti;

        int vi = (int) ((double) theta1_v * vi_pre + (double) theta0_v + value_list.get(i));
        value_list.set(i, vi);
        vi_pre = vi;
      }

      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i = 0; i < block_size - 1; i++) {
        int ti = (time_list.get(i) - time0) * td_common + time0;
        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(ti);
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }
      quickSort(ts_block, 0, 0, block_size - 1);
      data.addAll(ts_block);
    }

    if (remain_length == 1) {
      int timestamp_end = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value_end = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      ArrayList<Integer> ts_block_end = new ArrayList<>();
      ts_block_end.add(timestamp_end);
      ts_block_end.add(value_end);
      data.add(ts_block_end);
    }
    if (remain_length != 0 && remain_length != 1) {
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      float theta0_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta0_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list =
          decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, remain_length + zero_number);
      decode_pos += max_bit_width_time * (remain_length + zero_number - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list =
          decodebitPacking(
              encoded, decode_pos, max_bit_width_value, 0, remain_length + zero_number);
      decode_pos += max_bit_width_value * (remain_length + zero_number - 1) / 8;

      int td_common = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int ti_pre = time0;
      int vi_pre = value0;
      for (int i = 0; i < remain_length + zero_number - 1; i++) {
        int ti = (int) ((double) theta1_r * ti_pre + (double) theta0_r + time_list.get(i));
        time_list.set(i, ti);
        ti_pre = ti;

        int vi = (int) ((double) theta1_v * vi_pre + (double) theta0_v + value_list.get(i));
        value_list.set(i, vi);
        vi_pre = vi;
      }

      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i = 0; i < remain_length + zero_number - 1; i++) {
        int ti = (time_list.get(i) - time0) * td_common + time0;
        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(ti);
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }

      quickSort(ts_block, 0, 0, remain_length + zero_number - 1);

      for (int i = zero_number; i < remain_length + zero_number; i++) {
        data.add(ts_block.get(i));
      }
    }
    return data;
  }

  public static void main(String[] args) throws IOException {
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<Integer> dataset_block_size = new ArrayList<>();

    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\Metro-Traffic");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\Metro-Traffic_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\Nifty-Stocks");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\Nifty-Stocks_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\USGS-Earthquakes");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\USGS-Earthquakes_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\Cyber-Vehicle");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\Cyber-Vehicle_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\TH-Climate");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\TH-Climate_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\TY-Transport");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\TY-Transport_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\TY-Fuel");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\TY-Fuel_ratio.csv");
    input_path_list.add("E:\\thu\\TestTimeGrid\\test_path\\iotdb_test\\GW-Magnetic");
    output_path_list.add(
        "E:\\thu\\TestTimeGrid\\test_path\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\GW-Magnetic_ratio.csv");

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
          tmp.add(Integer.valueOf(loader.getValues()[0]));
          tmp.add(Integer.valueOf(loader.getValues()[1]));
          data.add(tmp);
        }
        inputStream.close();
        long encodeTime = 0;
        long decodeTime = 0;
        double ratio = 0;
        double compressed_size = 0;
        int repeatTime2 = 1;
        for (int i = 0; i < repeatTime; i++) {
          long s = System.nanoTime();
          ArrayList<Byte> buffer = new ArrayList<>();
          for (int repeat = 0; repeat < repeatTime2; repeat++)
            buffer = ReorderingRegressionEncoder(data, 192);
          long e = System.nanoTime();
          encodeTime += ((e - s) / repeatTime2);
          compressed_size += buffer.size();
          double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
          ratio += ratioTmp;
          s = System.nanoTime();
          for (int repeat = 0; repeat < repeatTime2; repeat++)
            data_decoded = ReorderingRegressionDecoder(buffer);
          e = System.nanoTime();
          decodeTime += ((e - s) / repeatTime2);
        }

        ratio /= repeatTime;
        compressed_size /= repeatTime;
        encodeTime /= repeatTime;
        decodeTime /= repeatTime;

        String[] record = {
          f.toString(),
          "MultipleGridNumRaw",
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
