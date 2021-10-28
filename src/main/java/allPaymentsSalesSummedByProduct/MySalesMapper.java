package allPaymentsSalesSummedByProduct;

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

		String regex = "\"13,000\"";
		String replacement = "13000";
		String line = value.toString().replaceAll(regex, replacement);
		// line consists of tokens-> tokens= colons divided by ,
		StringTokenizer tokens = new StringTokenizer(line, ",");

		List<String> tokensList = new ArrayList<>();
		String typeOfProduct = null;
		int price = 0;

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
				if (i == 1) {
					typeOfProduct = tokensList.get(1);

				} else if (i == 2) {

					price = Integer.parseInt(tokensList.get(2));
				}

			}
			wordForPaymentType.set(typeOfProduct);

			output.collect(wordForPaymentType, new IntWritable(price));
		}

	}

}
