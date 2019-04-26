package task2;

import java.time.ZonedDateTime;
import java.util.Date;

public class Utils {

	public static boolean isMoreRecentThan10Years(String timestamp) {

		Date date = new Date(Long.parseLong(timestamp));
		ZonedDateTime now = ZonedDateTime.now();
		ZonedDateTime thirtyDaysAgo = now.plusYears(-10);
		if (date.toInstant().isBefore(thirtyDaysAgo.toInstant())) {
			return false;
		}
		return true;

	}

}
