package com.fraud.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip CSV header
        if (line.startsWith("transaction_id")) {
            return;
        }

        String[] fields = line.split(",", -1);

        // Validate expanded CSV record
        if (fields.length != 6) {
            return;
        }

        String accountId = fields[1].trim();

        try {
            double amount = Double.parseDouble(fields[2].trim());

            context.write(
                new Text(accountId),
                new DoubleWritable(amount)
            );

        } catch (NumberFormatException e) {
            // Skip invalid transaction amounts
        }
    }
}
