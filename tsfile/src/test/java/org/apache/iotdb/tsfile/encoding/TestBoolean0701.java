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

public class TestBoolean0701 {
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

  public static ArrayList<Byte> ReorderingRegressionEncoder(ArrayList<Boolean> data) {
    ArrayList<Byte> encoded_result = new ArrayList<>();

    int data_length = data.size();
    int remain_length = 0;

    if(data_length%8!=0){
      remain_length = 8 - data_length%8;
      for(int i=0;i<remain_length;i++){
        data.add(Boolean.FALSE);
      }
    }

    int block_num = data.size() / 8;

    byte[] result = new byte[block_num];
    for (int i = 0; i < block_num; i++) {
      int tmp_int = 0;
      for (int k = 0; k < 8; k++) {
        int num=0;
        if(data.get(i * 8 + k )==Boolean.TRUE){
          num=1;
        }
        tmp_int += (num<<k);
      }
      result[i] = (byte) tmp_int;
    }

    byte[] remain_length_byte = int2Bytes(remain_length);
    for (byte b : remain_length_byte) encoded_result.add(b);

    for (byte b : result) encoded_result.add(b);

    return encoded_result;
  }

  public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded) {
    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
    return data;
  }

  public static void main(String[] args) throws IOException {
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();

//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool1");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//          + "\\compression_ratio\\rr_ratio\\Bool1_ratio.csv");
//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool2");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//            + "\\compression_ratio\\rr_ratio\\Bool2_ratio.csv");
//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool3");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//            + "\\compression_ratio\\rr_ratio\\Bool3_ratio.csv");
//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool4");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//            + "\\compression_ratio\\rr_ratio\\Bool4_ratio.csv");

    //input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool-Bot");
    //output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
    //        + "\\compression_ratio\\rr_ratio\\Bool-Bot_ratio.csv");
    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool-Gol");
    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
            + "\\compression_ratio\\rr_ratio\\Bool-Gol_ratio.csv");
//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool-Institution");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//            + "\\compression_ratio\\rr_ratio\\Bool-Institution_ratio.csv");
//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool-bd2");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//            + "\\compression_ratio\\rr_ratio\\Bool-bd2_ratio.csv");
//    input_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\iotdb_test\\Bool-Estimate");
//    output_path_list.add("E:\\thu\\TimeEncoding\\TestTimeGrid\\result_python\\result_evaluation"
//            + "\\compression_ratio\\rr_ratio\\Bool-Estimate_ratio.csv");

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
        ArrayList<Boolean> data = new ArrayList<>();
        ArrayList<Integer> data_decoded = new ArrayList<>();

        loader.readHeaders();
        data.clear();
        while (loader.readRecord()) {
          //ArrayList<Boolean> tmp = new ArrayList<>();
          //tmp.add(Boolean.valueOf(loader.getValues()[0]));
          //tmp.add(Boolean.valueOf(loader.getValues()[1]));
          data.add(Boolean.valueOf(loader.getValues()[0]));
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
            buffer = ReorderingRegressionEncoder(data);
          }
          long e = System.nanoTime();
          encodeTime += ((e - s) / repeatTime2);

          byte[] elems = new byte[buffer.size()];
          for (int b = 0; b < buffer.size(); b++) {
            elems[i] = buffer.get(i);
          }
          byte[] compressed = compressor.compress(elems);
          compressed_size += compressed.length;
          double ratioTmp = (double) ((compressed.length+1) * Integer.BYTES ) / (double) (data.size() * Integer.BYTES );

          System.out.println(data.size() * Integer.BYTES);
          //System.out.print(" ");
          System.out.println(compressed.length * Integer.BYTES);

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
        //System.out.println(ratio);
        writer.writeRecord(record);
      }
      writer.close();
    }
  }
}
