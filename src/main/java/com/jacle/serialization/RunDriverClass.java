package com.jacle.serialization;

import com.jacle.serialization.avsc.AvroMapReduce;
import org.apache.hadoop.util.ProgramDriver;

public class RunDriverClass
{
    public static void main(String[] args)
    {
        ProgramDriver programDriver=new ProgramDriver();
        int exitCode=0;
        try {
            programDriver.addClass("avro", AvroMapReduce.class,"avro mapreduce");

            exitCode=programDriver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
