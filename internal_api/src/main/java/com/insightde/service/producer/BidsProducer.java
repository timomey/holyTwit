package com.insightde.service.producer;

import static org.apache.log4j.Logger.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.insightde.service.restful.TaggerController;
import com.insightde.utils.IodineJsonParser;
import com.insightde.utils.Nums;
import com.insightde.utils.clients.Rest;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import sun.rmi.runtime.Log;

import org.fluentd.logger.FluentLogger;

@Service
@ConditionalOnProperty(name = "simulate.bidding.produce")
public class BidsProducer {

	final static org.apache.log4j.Logger logger = getLogger(BidsProducer.class);
	private static final Config conf = ConfigFactory.load();
	private IodineJsonParser isp = new IodineJsonParser(conf.getString("tagging.iodine.files.directory"));
	private Map<String, Object> pharmaBids = Maps.newTreeMap();
	private static FluentLogger LOG = FluentLogger.getLogger("pharma.bids");
	private static final String ES_HOST = conf.getString("elasticsearch.host");

	// get sorted list of all drug comps
	public BidsProducer() {
	}

	// simulate bidding by pharma comps
	// run this every 1 s
	@Scheduled(fixedRate = 1000)
	public void produceBidsSimulation() {
		for (String drug_comp : IodineJsonParser.getAll_drug_companies_list()) {
			Map<String, Object> bidItem = new HashMap<String, Object>();
			bidItem.put("pharmatags", drug_comp);
			bidItem.put("price", Nums.round(Nums.getRandomNumberInRange(1, 15) / 100.00, 2));
			LOG.log("prices", bidItem);
		}
	}

	// run every 3 days
	@Scheduled(fixedRate = 259200)
	public void updateDrugCompaniesOrdering() {
		List<String> difn = new ArrayList<String>(IodineJsonParser.getAll_drug_companies_list());
		String response = Rest.post("http://" + ES_HOST + ":9200/_search", readElasticSearchRequestFile());
		ReadContext ctx = JsonPath.parse(response);
		List<String> top_drug_companies_lst = new ArrayList<String>(ctx.read("$.aggregations.2.buckets[*].key"));
		difn.removeAll(top_drug_companies_lst);
		List<String> newOrderings = new ArrayList<String>(top_drug_companies_lst);
		newOrderings.addAll(difn);
		IodineJsonParser.setAll_drug_companies_list(newOrderings);
		logger.info(newOrderings.size());
	}

	private String readElasticSearchRequestFile() {
		String content = "";
		try {
			content = new String(
					Files.readAllBytes(Paths.get("src/main/resources/request_top_bidder_from_elasticsearch.json")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}

}
