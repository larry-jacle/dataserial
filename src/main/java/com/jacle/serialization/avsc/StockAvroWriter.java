package com.jacle.serialization.avsc;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class StockAvroWriter {
    public static void main(String[] args) throws IOException {
        //进行对象序列化
        //对象有三种初始化，构造方法、set/get、build方法
        StockAvroBean stock = StockAvroBean.newBuilder().setStockCode("001")
                .setStockName("stock1").setCurrentPrice(12.3f)
                .setOpenPrice(12.9f).setPreclosePrice(10.9f).setTradeTime(12312300l)
                .build();

        StockAvroWriter stockAvroWriter = new StockAvroWriter();
//        StockAvroBean stockAvroBean = stockAvroWriter.deSerialization("d:/avrofile.avro");

        byte[] bytes=stockAvroWriter.avroSerialization(stock);
        StockAvroBean avroBean=stockAvroWriter.deSerialization(bytes);
        System.out.println(avroBean);
    }

    /**
     * 序列化为对象文件
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
     * @param stock
     * @return
     * @throws IOException
     */
    public byte[] avroSerialization(StockAvroBean stock) throws IOException {
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        DatumWriter<StockAvroBean> dataumWriter = new SpecificDatumWriter<StockAvroBean>();
        DataFileWriter<StockAvroBean> dataWriter = new DataFileWriter<StockAvroBean>(dataumWriter);

        //创建序列化文件，append追加内容，关闭连接
        dataWriter.create(stock.getSchema(), bytes);
        dataWriter.append(stock);
        dataWriter.close();

        return bytes.toByteArray();
    }

    /**
     * 反序列
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
        SeekableByteArrayInput byteArrayInputStream=new SeekableByteArrayInput(bytes);
        DatumReader<StockAvroBean> datumReader = new SpecificDatumReader<StockAvroBean>(StockAvroBean.class);
        DataFileReader<StockAvroBean> dataFileReader = new DataFileReader<StockAvroBean>(byteArrayInputStream, datumReader);

        if (dataFileReader.hasNext()) {
            StockAvroBean stockAvroBean = dataFileReader.next();
            return stockAvroBean;
        }

        return null;
    }
}
