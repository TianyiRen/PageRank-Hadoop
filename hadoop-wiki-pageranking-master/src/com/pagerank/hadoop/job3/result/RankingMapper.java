package com.pagerank.hadoop.job3.result;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankingMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {

	public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter arg3)
			throws IOException {
		String[] pageAndRank = getPageAndRank(key, value);

		float parseFloat = Float.parseFloat(pageAndRank[1]);

		Text page = new Text(pageAndRank[0]);
		FloatWritable rank = new FloatWritable(parseFloat);

		output.collect(rank, page);
	}

	private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
		String[] pageAndRank = new String[2];
		int tabPageIndex = value.find("\t");
		int tabRankIndex = value.find("\t", tabPageIndex + 1);
		int tabOutgoingPageIndex = value.find("\t", tabRankIndex + 1);

		// no tab after rank (when there are no links)
		int end;
		if (tabRankIndex == -1) {
			tabOutgoingPageIndex++;
			end = value.getLength() - (tabPageIndex + 1);
		} else {
			end = tabRankIndex - (tabPageIndex + 1);
		}

		String outgoingPageList = Text.decode(value.getBytes(), tabOutgoingPageIndex + 1, value.getLength()
				- (tabOutgoingPageIndex + 1));

		int startPageCategoriesIndex = outgoingPageList.indexOf("#");
		int endPageCategoriesIndex = outgoingPageList.lastIndexOf("#");
		String pageCategories = outgoingPageList.substring(startPageCategoriesIndex + 1, endPageCategoriesIndex);

		pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex) + " " + pageCategories;
		pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);

		return pageAndRank;
	}

}
