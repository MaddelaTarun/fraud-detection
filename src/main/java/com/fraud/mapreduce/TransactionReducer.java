package com.fraud.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TransactionReducer
        extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text accountId, Iterable<DoubleWritable> amounts,
                          Context context)
            throws IOException, InterruptedException {

        int transactionCount = 0;
        double totalAmount = 0.0;

        for (DoubleWritable amount : amounts) {
            transactionCount++;
            totalAmount += amount.get();
        }

        double averageAmount = totalAmount / transactionCount;

        String result = transactionCount + ","
                + totalAmount + ","
                + averageAmount;

        context.write(accountId, new Text(result));
    }
}
