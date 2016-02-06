package com.insightde.utils;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class IodineJsonParserTest {
	
	private static final Config conf = ConfigFactory.load();

	@Test
	public void testOpenConditionsDir() {
		IodineJsonParser isp = new IodineJsonParser(conf.getString("tagging.iodine.files.directory"));
		Map<String,Set<String>> symptoms_condititions = isp.getSymptoms_conditions();
		Set<String> conditions = symptoms_condititions.get("Flu-like symptoms");
		assertTrue(conditions.contains("HIV"));
		
//		Set<String>symptoms = isp.getAllSymptoms();
		
	}
	
}
