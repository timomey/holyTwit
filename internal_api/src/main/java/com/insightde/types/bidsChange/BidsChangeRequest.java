package com.insightde.types.bidsChange;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;


public class BidsChangeRequest {
	private static final Logger logger = LoggerFactory.getLogger(BidsChangeRequest.class);
	private static ObjectMapper mapper;
	List<String>bidsOrder = Lists.newArrayList();

	public List<String> getBidsOrder() {
		return bidsOrder;
	}

	public void setBidsOrder(List<String> bidsOrder) {
		this.bidsOrder = bidsOrder;
	}
	
	
	public static BidsChangeRequest jsonToPojo(String msg) {
		mapper = new ObjectMapper();
		try {
			BidsChangeRequest b = mapper.readValue(msg, BidsChangeRequest.class);
			return b;
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
		return null;
	}

}
