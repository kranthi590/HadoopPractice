package com.hadoop.examples;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.hadoop.utils.HadoopConstants;

public class HdfsConnector {

	private static Logger logger = Logger.getLogger(HdfsConnector.class);
	private FileSystem fileSystem = null;
	private Configuration conf = null;

	public HdfsConnector(String uri) throws IOException {
		conf = new Configuration();
		fileSystem = FileSystem.get(URI.create(uri), conf);
	}

	public boolean isServerReachable() {
		Socket socket = null;
		boolean reachable = false;
		try {
			socket = new Socket(HadoopConstants.SERVER_IP, HadoopConstants.SERVER_PORT);
			reachable = true;
			logger.info("Connected to Server  " + HadoopConstants.SERVER_IP + " with port ["
					+ HadoopConstants.SERVER_PORT + "]");
		} catch (Exception e) {

			logger.info("Failed to connect Server  " + HadoopConstants.SERVER_IP + " with port ["
					+ HadoopConstants.SERVER_PORT + "]");
		} finally {
			if (socket != null)
				try {
					socket.close();
				} catch (IOException e) {
				}
		}
		return reachable;
	}

	public boolean isFileExists(String filePath) {

		boolean isFileExists = false;

		try {
			Path inFile = new Path(filePath);
			if (!fileSystem.exists(inFile))
				isFileExists = false;
			else if (!fileSystem.isFile(inFile))
				isFileExists = false;
			else
				isFileExists = true;
		} catch (Exception e) {
			logger.error("Exception: " + e.getMessage());

		}
		return isFileExists;
	}

}
