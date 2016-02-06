package com.insightde.service.producer;

import static org.junit.Assert.*;

import org.junit.Test;

public class BidsProducerTest {

	@Test
	public void testUpdateDrugCompaniesOrdering() {
		BidsProducer bp = new BidsProducer();
		bp.updateDrugCompaniesOrdering();
	}

}
