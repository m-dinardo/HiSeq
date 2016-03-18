package io.github.mdinardo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

public class WriteHiSeq {

	public static void main(String[] args) throws IOException {
		String in = args[0];
		String out = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(in), conf);
		FileStatus[] files = fs.listStatus(new Path(in));
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				new Path(out), Text.class, BytesWritable.class);

		for (FileStatus file : files) {
			FSDataInputStream f = fs.open(file.getPath());
			Text k = new Text(file.getPath().toString());
			BytesWritable v = new BytesWritable(
					org.apache.commons.io.IOUtils.toByteArray(f));
			writer.append(k, v);
		}

		IOUtils.closeStream(writer);
	}
}
