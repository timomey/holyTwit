package com.insightde.taggers.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static org.apache.log4j.Logger.getLogger;

public class RuleEntryParser {
	
	private static final Logger logger = getLogger(RuleEntryParser.class);
	private static Config conf = ConfigFactory.load();
	
	//CSV file header
    private static ArrayList<String>FILE_HEADER_MAPPING = Lists.newArrayList();
    private static int rulesSize;  //=conf.getInt("tagging."+dictName+".rulesset.size");
	
	//Rule attributes
	private static final String MAIN_THEME = "mainTheme";
	private static final String SUB_THEME = "subTheme";
	private static final String COMMENTS = "Comments";
	
	public RuleEntryParser(String dictName){
		rulesSize=conf.getInt("tagging."+dictName+".rulesset.size");
		FILE_HEADER_MAPPING.add("mainTheme");
    	FILE_HEADER_MAPPING.add("subTheme");
    	FILE_HEADER_MAPPING.add("Comments");
    	for(int i=1;i<=rulesSize;i++){
    		FILE_HEADER_MAPPING.add("Filename"+new Integer(i).toString());
    		FILE_HEADER_MAPPING.add("Rule"+new Integer(i).toString());
    	}
    	FILE_HEADER_MAPPING.add("Filename"+new Integer(rulesSize+1).toString());
    	rulesSize = conf.getInt("tagging."+dictName+".rulesset.size");
	}
	
	public  List<Rule> readCsvFile(FileReader fileReader) {

		CSVParser csvFileParser = null;
		
		//Create the CSVFormat object with the header mapping
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader( FILE_HEADER_MAPPING.toArray(new String[FILE_HEADER_MAPPING.size()]));
     
        //Create a new list of rules to be filled by CSV file data 
        List<Rule> rules = new ArrayList();
        try {
            
            //initialize CSVParser object
            csvFileParser = new CSVParser(fileReader, csvFileFormat);
            
            //Get a list of CSV file records
            List csvRecords = csvFileParser.getRecords(); 
            
            //Read the CSV file records starting from the second record to skip the header
            for (int i = 1; i < csvRecords.size(); i++) {
            	CSVRecord record = (CSVRecord) csvRecords.get(i);
//            	logger.info(record);
            	
            	Rule rule = new Rule( record.get(MAIN_THEME), record.get(SUB_THEME));
            	List<String> booleans = rule.getRules();
            	List<String> filenames = rule.getFilenames();
            	String ruleString = rule.getRule();
            	logger.info(record);
            	int k=0;
            	for(int j=0; j<rulesSize && k<=rulesSize+3+2; j++){
            		String item = record.get(j+3);
            		k=j+3;
            		if(item!=null && !item.equals("")){
	            		if((k)%2!=0){
	            			String[] splitted  = item.trim().split(".csv");
	            			filenames.add(splitted[0]);
	            			ruleString+=splitted[0]+" ";
	            			
	            		}else{
	            			if(!item.equals("")){
	            				booleans.add(item.trim());
	            				ruleString+=item.trim()+" ";
	            			}
	            		}
            		}
            	}
            	if(record.get(k)!=null && !record.get(k).equals("")){
            		String[] splitted  = record.get(rulesSize+3+1).trim().split(".csv");
            		filenames.add(splitted[0]);
            		ruleString+=splitted[0];
            	}
            	rule.setRule(ruleString);
            	rule.setRules(booleans);
            	rule.setFilenames(filenames);
//            	logger.info(rule);
                rules.add(rule);	
			}
            logger.info(rules);
        } catch (Exception e) {
        	logger.error(e.getMessage());
        } finally {
            try {
                fileReader.close();
                csvFileParser.close();
            } catch (IOException e) {
            	logger.error(e.getStackTrace());
            }
        }
        return rules;
	}

}