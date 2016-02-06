package com.insightde.service.restful;

import static org.apache.log4j.Logger.getLogger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
//import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.insightde.ApplicationModeType;
import com.insightde.taggers.services.Tagger;
import com.insightde.taggers.services.impl.IodineLuwakTagger;
import com.insightde.types.TT.response.Tweet;
import com.insightde.types.reddit.Reddit;
import com.insightde.types.service.restful.response.TaggerResponse;
import com.insightde.types.sources.DataSourceType;
import com.insightde.utils.IodineJsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RestController
@ConditionalOnProperty(name = "tagger.webservice")
public class TaggerController {

	final static org.apache.log4j.Logger logger = getLogger(TaggerController.class);
	private static Config conf = ConfigFactory.load();

	public static final ApplicationModeType applicationMode = ApplicationModeType
			.findByShortName(conf.getString("application.mode"));
	public static final String applicationModeName = applicationMode.getLongname();

	private Tagger tagger;

	public TaggerController() {
		tagger = new IodineLuwakTagger();
	}

	@RequestMapping(value = "/tagbatch/{socialMediaType}", method = RequestMethod.POST)
	public String processMapBatch(@RequestParam(value = "datasource", required = false) String datasource,
			@RequestBody String jsonString, @PathVariable String socialMediaType)
					throws JsonParseException, JsonMappingException, IOException {
		String taggedResponse="";
		DataSourceType ds = DataSourceType.findDSbyShortname(socialMediaType);
		if ( ds== DataSourceType.RD) {
			Map<String, Reddit> payload = Maps.newHashMap();
			payload.put("0", new ObjectMapper().readValue(jsonString, Reddit.class));
			Map<String, Reddit> enriched = tagger.enrichPost(payload);
			TaggerResponse tr = new TaggerResponse(ds);
			tr.setTaggerRespReddit(enriched);
			taggedResponse =  tr.toString();
		}else if(ds== DataSourceType.TT){
			Map<String, Tweet> payload = Maps.newHashMap();
			Tweet t = new ObjectMapper().readValue(jsonString, Tweet.class);
			payload.put("0",t );
			Map<String, Tweet> enriched = tagger.enrichPost(payload);
			TaggerResponse tr = new TaggerResponse(ds);
			tr.setTaggerRespTweet(enriched);
			taggedResponse = tr.toString();
		}
		logger.info(">>>>> "+taggedResponse);
		return taggedResponse;
	}
}
