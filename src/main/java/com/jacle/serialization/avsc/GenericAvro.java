package com.jacle.serialization.avsc;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * 可以通过非Avro生成类来处理avro序列化
 * 通过genericRecord来进行序列化和反序列化
 */
public class GenericAvro
{
    public static void main(String[] args) throws IOException {
        //通过class的getresource来获取资源
        //getresource只能指定到文件夹
//        System.out.println(GenericAvro.class.getResource(".").getPath());
        File avscFile=new File(new File(GenericAvro.class.getResource("/").getPath()).getParent(),"conf/stock.avsc");

        //先获取avsc模式文件获取schema
        System.out.println(avscFile);
        Schema schema=new Schema.Parser().parse(avscFile);
        System.out.println(schema);

//        {"name":"stockCode","type":"string"},
//        {"name":"stockName","type":"string"},
//        {"name":"tradeTime","type":"long"},
//        {"name":"preclosePrice","type":"float"},
//        {"name":"openPrice","type":"float"},
//        {"name":"currentPrice","type":"float"}

        GenericRecord record=new GenericData.Record(schema);
        record.put("stockCode","003");
        record.put("stockName","name003");
        record.put("tradeTime",12321321l);
        record.put("preclosePrice",19.9f);
        record.put("openPrice",19.9f);
        record.put("currentPrice",19.9f);

        File file=new File("d:/stocknew.avro");
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema,file);
        dataFileWriter.append(record);
        dataFileWriter.close();

        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord stock = null;
        while(dataFileReader.hasNext()) {
            stock = dataFileReader.next(stock);
            System.out.println(stock);
        }

    }
}
