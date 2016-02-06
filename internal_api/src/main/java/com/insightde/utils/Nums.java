package com.insightde.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

public class Nums {

	public static Double getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return new Double(r.nextInt((max - min) + 1) + min);
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
}

