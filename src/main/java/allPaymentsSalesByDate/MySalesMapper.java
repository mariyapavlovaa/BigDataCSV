package allPaymentsSalesByDate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MySalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	Text wordForPaymentType = new Text();

	@Override
	// rowNumber // line
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		// line consists of tokens-> tokens= colons divided by ,
		StringTokenizer tokens = new StringTokenizer(value.toString(), ",");

		List<String> tokensList = new ArrayList<>();
		String date = null;

		while (tokens.hasMoreTokens()) {
			String token = tokens.nextToken();
			if (token.equals("Transaction_date")) {
				return;
			} else {
				tokensList.add(token);
			}

		}
		if (!tokensList.isEmpty()) {
			for (int i = 0; i < tokensList.size(); i++) {
				if (i == 0) {
					String regex = "(?<month>[0-9]+)(\\.|\\/)(?<day>[0-9]+)(\\.|\\/)(?<year>[0-9]+)";
					
					date = tokensList.get(i);
					wordForPaymentType.set(date);
					output.collect(wordForPaymentType, new IntWritable(1));
				}
			}

		}

	}

}
