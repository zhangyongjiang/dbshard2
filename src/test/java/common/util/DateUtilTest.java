package common.util;

import java.util.Calendar;

import org.junit.Assert;
import org.junit.Test;

public class DateUtilTest {
	@Test
	public void testDayOfMonth() {
		Calendar cal = DateUtil.dayOfMonth(2015, 1, 10);
		Assert.assertEquals(2015, cal.get(Calendar.YEAR));
		Assert.assertEquals(0, cal.get(Calendar.MONTH));
		Assert.assertEquals(10, cal.get(Calendar.DAY_OF_MONTH));
	}
	
	@Test
	public void testDayOfPreviousMonth() {
		Calendar cal = DateUtil.dayOfPreviousMonth(2015, 3, 10);
		Assert.assertEquals(2015, cal.get(Calendar.YEAR));
		Assert.assertEquals(1, cal.get(Calendar.MONTH));
		Assert.assertEquals(10, cal.get(Calendar.DAY_OF_MONTH));
	}
}
