package com.capstone.fraudetection.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Utility class for calculating the milliseconds for the date
 *
 */
public class DateUtility {

	public static Long getMilliseconds(String date) {
		final String strDate = date;
		Long millis = 0l;
		try {
			millis = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss").parse(strDate).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return millis;
	}

}
