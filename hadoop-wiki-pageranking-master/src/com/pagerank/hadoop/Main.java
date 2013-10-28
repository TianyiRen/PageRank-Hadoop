package com.pagerank.hadoop;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.pagerank.hadoop.job1.xmlinputformatter.LinksMapper;
import com.pagerank.hadoop.job1.xmlinputformatter.LinksReducer;
import com.pagerank.hadoop.job1.xmlinputformatter.XmlInputFormat;
import com.pagerank.hadoop.job2.calculate.RankCalculateMapper;
import com.pagerank.hadoop.job2.calculate.RankCalculateReduce;
import com.pagerank.hadoop.job3.result.RankingMapper;

public class Main {

	private static NumberFormat nf = new DecimalFormat("00");

	public static void main(String[] args) throws Exception {

		Options options = createOptions();
		CommandLine cmd = null;
		try {
			cmd = new PosixParser().parse(options, args);
		} catch (ParseException ex) {
			System.out.println("Error parsing the options");
		}

		if (!cmd.hasOption("input") || !cmd.hasOption("interimResult") || !cmd.hasOption("finalResult")) {
			return;
		}

		String inputPath = cmd.getOptionValue("input");
		inputPath = appendTrailingSlash(inputPath);

		String interimResultPath = cmd.getOptionValue("interimResult");
		interimResultPath = appendTrailingSlash(interimResultPath);

		String finalResultPath = cmd.getOptionValue("finalResult");
		finalResultPath = appendTrailingSlash(finalResultPath);

		Main pageRanking = new Main();

		// String inputPath =
		// "s3n://page-rank-emr/data/nlwiki-latest-pages-articles.xml";
		// String interimResultPath = "s3n://page-rank-emr/interimResultsPR10/";
		// String finalResultPath = "s3n://page-rank-emr/resultsPR10/result/";

		// pageRanking.runXmlParsing("wiki/in", "wiki/ranking/iter00");
		pageRanking.runXmlParsing(inputPath, interimResultPath + "iter00");

		int runs = 0;
		for (; runs < 5; runs++) {
			pageRanking.runRankCalculation(interimResultPath + "iter" + nf.format(runs), interimResultPath + "iter"
					+ nf.format(runs + 1));
		}

		pageRanking.runRankOrdering(interimResultPath + "iter" + nf.format(runs), finalResultPath);

	}

	private static String appendTrailingSlash(String inputPath) {
		if (!inputPath.endsWith("/")) {
			inputPath += "/";
		}
		return inputPath;
	}

	@SuppressWarnings("static-access")
	private static Options createOptions() {
		Options options = new Options();
		options.addOption(new Option("help", "print this message"));
		options.addOption(OptionBuilder.withArgName("input").hasArg().withDescription("").create("input"));
		options.addOption(OptionBuilder.withArgName("interimResult").hasArg().withDescription("")
				.create("interimResult"));
		options.addOption(OptionBuilder.withArgName("finalResult").hasArg().withDescription("").create("finalResult"));

		return options;
	}

	public void runXmlParsing(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(Main.class);

		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		// Input / Mapper
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(XmlInputFormat.class);
		conf.setMapperClass(LinksMapper.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setReducerClass(LinksReducer.class);

		JobClient.runJob(conf);
	}

	private void runRankCalculation(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(Main.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(RankCalculateMapper.class);
		conf.setReducerClass(RankCalculateReduce.class);

		JobClient.runJob(conf);
	}

	private void runRankOrdering(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(Main.class);

		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(RankingMapper.class);

		JobClient.runJob(conf);
	}

}
