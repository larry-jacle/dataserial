package com.jacle.serialization.avsc;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * mr对avro文件的读取和写入
 */
public class AvroMapReduce
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);

        //设定run class
        job.setJarByClass(AvroMapReduce.class);
        job.setMapperClass(AvroMapper.class);
        job.setReducerClass(AvroReduce.class);

        //设定map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //注意输入输出类型
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        //设置输入输出的avro schema
        AvroJob.setInputKeySchema(job,StockAvroBean.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.LONG));


        //指定MR的输入输出路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //true表示运行信息显示给客户端，false只是等待任务结束
        //mr的运行是异步的，这一句一定要加
        boolean jobFlag=job.waitForCompletion(true);
        System.exit(jobFlag?0:1);
    }

    public static class AvroMapper extends Mapper<AvroKey<StockAvroBean>,NullWritable,Text,LongWritable>
    {
        @Override
        protected void map(AvroKey<StockAvroBean> key, NullWritable value, Context context) throws IOException, InterruptedException {
            //mapreduce自动的对avro进行切片，按照之间的同步标记作为split flag
            StockAvroBean stock=key.datum();
            context.write(new Text(stock.getStockName().toString()),new LongWritable(1));
        }
    }

    public static class AvroReduce extends Reducer<Text,LongWritable,AvroKey<CharSequence>,AvroValue<Long>>
    {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;

            for(LongWritable v:values)
            {
                sum=sum+v.get();
            }

            context.write(new AvroKey<CharSequence>(key.toString()),new AvroValue<Long>(sum));
        }
    }
}
