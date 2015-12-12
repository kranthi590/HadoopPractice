package com.hadoop.app;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.hadoop.examples.HdfsConnector;
import com.hadoop.utils.HadoopConstants;

public class App {

	private static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {

		HdfsConnector connector;
		try {
			connector = new HdfsConnector(HadoopConstants.URI);
			if (connector.isServerReachable()) {
				String path = "/data/sample";
				logger.info("Is file eixts: " + HadoopConstants.URI + path + " :" + connector.isFileExists(path));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
