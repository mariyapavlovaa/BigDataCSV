package allPaymentsSalesByDate;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
	Text wordForSaleDate = new Text();

	@Override
	// rowNumber // line
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		// line consists of tokens-> tokens= colons divided by ,
		StringTokenizer tokens = new StringTokenizer(value.toString(), ",");

		List<String> tokensList = new ArrayList<>();

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
					// String regex =
					// "(?<month>[0-9]+)(\\.)(?<day>[0-9]+)(\\.)(?<year>[0-9]+)(\\s+)(?<hours>[0-9]+)(\\:)(?<minutes>[0-9]+)";
					StringTokenizer dateAndTime = new StringTokenizer(tokensList.get(i), " ");
					String dateOnly = dateAndTime.nextToken();

					SimpleDateFormat toFixFormat = new SimpleDateFormat("MM.dd.yyyy");
					SimpleDateFormat targetFormat = new SimpleDateFormat("MM/dd/yyyy");
					Date date = null;

					try {
						date = toFixFormat.parse(dateOnly);
						targetFormat.format(date);

					} catch (ParseException e) {
						try {
							date = targetFormat.parse(dateOnly);
						} catch (ParseException e1) {
							e1.printStackTrace();
						}
						// e.printStackTrace();
					}

					Calendar cal = Calendar.getInstance();
				    cal.setTime(date);
				    String formatedDate = cal.get(Calendar.DATE) + "/" + 
				            (cal.get(Calendar.MONTH) + 1) + 
				            "/" +         cal.get(Calendar.YEAR);
					wordForSaleDate.set(formatedDate);
					output.collect(wordForSaleDate, new IntWritable(1));
				}
			}

		}

	}

//	public static String dateConversion(String inputPattern, String outputPattern, String givenDate)
//			throws ParseException {
//
//		SimpleDateFormat inputFormat = new SimpleDateFormat(inputPattern);
//		SimpleDateFormat outputFormat = new SimpleDateFormat(outputPattern);
//
//		Date date = null;
//		String requiredDate = null;
//
//		date = inputFormat.parse(givenDate);
//		requiredDate = outputFormat.format(date);
//
//		return requiredDate;
//
//	}

}
