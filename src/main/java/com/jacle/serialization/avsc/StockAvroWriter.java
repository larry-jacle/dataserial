package com.jacle.serialization.avsc;

import com.google.common.primitives.Bytes;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hdfs.util.ByteArray;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 一般程序用到的泛型，定义在类的级别上
 *
 * @param <T>
 */
public class StockAvroWriter<T extends SpecificRecordBase> {
    public static void main(String[] args) throws IOException {
        //进行对象序列化
        //对象有三种初始化，构造方法、set/get、build方法
        StockAvroBean.Builder builder = StockAvroBean.newBuilder();
        StockAvroBean stock = builder.setStockCode("001")
                .setStockName("股票1").setCurrentPrice(12.3f)
                .setOpenPrice(12.9f).setPreclosePrice(10.9f).setTradeTime(12312300l)
                .build();
        StockAvroBean stock2 = builder.setStockCode("001")
                .setStockName("股票2").setCurrentPrice(12.3f)
                .setOpenPrice(12.9f).setPreclosePrice(10.9f).setTradeTime(12312300l)
                .build();

        ArrayList<StockAvroBean> stockList = new ArrayList<StockAvroBean>();
        stockList.add(stock);
        stockList.add(stock2);

        for(int i=0;i<1000000;i++)
        {
            stockList.add(builder.setStockCode("00"+i)
                    .setStockName("股票"+i).setCurrentPrice(12.3f)
                    .setOpenPrice(12.9f).setPreclosePrice(10.9f).setTradeTime(12312300l)
                    .build());
        }

        StockAvroWriter stockAvroWriter = new StockAvroWriter();
        StockAvroBean stockAvroBean = stockAvroWriter.deSerialization("D:\\avro-convert\\stock.avro");
        System.out.println("first avro bean json:"+stockAvroBean.toString());

        byte[] bytes=stockAvroWriter.avroSerialization(stock);
        StockAvroBean avroBean=stockAvroWriter.deSerialization(bytes);
        System.out.println(avroBean);
        System.out.println(bytes.length);

        //使用二进制的序列化明显内容要小很多
        byte[] bytes2=stockAvroWriter.avroSerializationBinary(stock);
        System.out.println(bytes2.length);

        byte[] jsonBytes = stockAvroWriter.avroListSerialization(stockList);
        System.out.println(jsonBytes.length);

        byte[] binaryBytes = stockAvroWriter.avroListSerializationBinary(stockList);
        System.out.println(binaryBytes.length);

    }

    /**
     * 序列化为对象文件
     *
     * @param stock
     * @param filePath
     * @throws IOException
     */
    public void avroSerialization(StockAvroBean stock, String filePath) throws IOException {
        DatumWriter<StockAvroBean> dataumWriter = new SpecificDatumWriter<StockAvroBean>();
        DataFileWriter<StockAvroBean> dataWriter = new DataFileWriter<StockAvroBean>(dataumWriter);

        //创建序列化文件，append追加内容，关闭连接
        dataWriter.create(stock.getSchema(), new File(filePath));
        dataWriter.append(stock);
        dataWriter.close();
    }


    /**
     * 序列化为二进制字节码
     *
     * @param stock
     * @return
     * @throws IOException
     */
    public byte[] avroSerialization(StockAvroBean stock) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DatumWriter<StockAvroBean> dataumWriter = new SpecificDatumWriter<StockAvroBean>();
        DataFileWriter<StockAvroBean> dataWriter = new DataFileWriter<StockAvroBean>(dataumWriter);

        //创建序列化文件，append追加内容，关闭连接
        dataWriter.create(stock.getSchema(), bytes);
        dataWriter.append(stock);
        dataWriter.close();

        return bytes.toByteArray();
    }

    public byte[] avroListSerialization(List<T> list) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DatumWriter<T> dataumWriter = new SpecificDatumWriter<T>();
        DataFileWriter<T> dataWriter = new DataFileWriter<T>(dataumWriter);

        //创建序列化文件，append追加内容，关闭连接
        dataWriter.create(list.get(0).getSchema(), bytes);
        for (T t : list) {
            dataWriter.append(t);
        }
        dataWriter.close();

        return bytes.toByteArray();
    }

    /**
     * 二进制序列化编码方式
     *
     * @param stock
     * @return
     * @throws IOException
     */
    public byte[] avroSerializationBinary(StockAvroBean stock) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DatumWriter<StockAvroBean> dataumWriter = new SpecificDatumWriter<StockAvroBean>(stock.getSchema());
//        BinaryEncoder binaryEncoder= EncoderFactory.get().directBinaryEncoder(bytes,null);

        //使用binaryEncoder要及时flush
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(bytes, null);
        //创建序列化文件，append追加内容，关闭连接
        dataumWriter.write(stock, binaryEncoder);
        binaryEncoder.flush();
        byte[] returnBytes = bytes.toByteArray();

        bytes.close();
        return returnBytes;
    }

    public byte[] avroListSerializationBinary(List<T> list) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DatumWriter<T> dataumWriter = new SpecificDatumWriter<T>(list.get(0).getSchema());
//        BinaryEncoder binaryEncoder= EncoderFactory.get().directBinaryEncoder(bytes,null);

        //使用binaryEncoder要及时flush
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(bytes, null);
        //创建序列化文件，append追加内容，关闭连接
        for (T t : list) {
            dataumWriter.write(t, binaryEncoder);
        }
        binaryEncoder.flush();
        byte[] returnBytes = bytes.toByteArray();

        bytes.close();
        return returnBytes;
    }


    /**
     * 反序列
     *
     * @param filepath
     * @return
     * @throws IOException
     */
    public StockAvroBean deSerialization(String filepath) throws IOException {
        File file = new File(filepath);
        DatumReader<StockAvroBean> datumReader = new SpecificDatumReader<StockAvroBean>(StockAvroBean.class);
        DataFileReader<StockAvroBean> dataFileReader = new DataFileReader<StockAvroBean>(file, datumReader);

        if (dataFileReader.hasNext()) {
            StockAvroBean stockAvroBean = dataFileReader.next();
            return stockAvroBean;
        }

        return null;
    }

    public StockAvroBean deSerialization(byte[] bytes) throws IOException {
        SeekableByteArrayInput byteArrayInputStream = new SeekableByteArrayInput(bytes);
        DatumReader<StockAvroBean> datumReader = new SpecificDatumReader<StockAvroBean>(StockAvroBean.class);
        DataFileReader<StockAvroBean> dataFileReader = new DataFileReader<StockAvroBean>(byteArrayInputStream, datumReader);

        if (dataFileReader.hasNext()) {
            StockAvroBean stockAvroBean = dataFileReader.next();
            return stockAvroBean;
        }

        return null;
    }

    public StockAvroBean deSerializationBinary(byte[] bytes) throws IOException {
        //json采用的是seekable
//        SeekableByteArrayInput byteArrayInputStream=new SeekableByteArrayInput(bytes);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DatumReader<StockAvroBean> datumReader = new SpecificDatumReader<StockAvroBean>(StockAvroBean.class);

        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteArrayInputStream, null);

        StockAvroBean stockAvroBean = datumReader.read(null, decoder);
        byteArrayInputStream.close();

        return stockAvroBean;
    }
}
