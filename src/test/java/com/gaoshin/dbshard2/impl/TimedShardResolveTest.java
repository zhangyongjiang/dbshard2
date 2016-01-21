package com.gaoshin.dbshard2.impl;

import java.text.ParseException;

import org.junit.Assert;
import org.junit.Test;

public class TimedShardResolveTest {
    @Test
    @SuppressWarnings("rawtypes")
    public void testMonthlyShardResolver() throws ParseException {
        MonthlyShardResolver r = new MonthlyShardResolver<>();
        r.setStartTime("20150101000000");
        Assert.assertEquals(0, r.getShardIdForTime("20150101000000"));
        Assert.assertEquals(0, r.getShardIdForTime("20150101000001"));
        Assert.assertEquals(0, r.getShardIdForTime("20150102000000"));
        Assert.assertEquals(1, r.getNumberOfShards("20150101000000"));
        Assert.assertEquals(1, r.getNumberOfShards("20150131000000"));
        
        Assert.assertEquals(1, r.getShardIdForTime("20150201000000"));
        Assert.assertEquals(2, r.getNumberOfShards("20150221000000"));
        
        Assert.assertEquals(12, r.getShardIdForTime("20160101000000"));
        Assert.assertEquals(14, r.getNumberOfShards("20160221000000"));
        
        Assert.assertEquals("20160201000000", r.getShardDateString("20160221000000"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testYearlyShardResolver() throws ParseException {
        YearlyShardResolver r = new YearlyShardResolver<>();
        r.setStartTime("20130101000000");
        Assert.assertEquals(0, r.getShardIdForTime("20130101000000"));
        Assert.assertEquals(1, r.getShardIdForTime("20140101000001"));
        Assert.assertEquals(2, r.getNumberOfShards("20140131000000"));
        
        Assert.assertEquals("20160101000000", r.getShardDateString("20160821000000"));
    }


    @Test
    @SuppressWarnings("rawtypes")
    public void testDailyShardResolver() throws ParseException {
        DailyShardResolver r = new DailyShardResolver<>();
        r.setStartTime("20160101000000");
        Assert.assertEquals(0, r.getShardIdForTime("20160101000000"));
        Assert.assertEquals(1, r.getShardIdForTime("20160102000001"));
        Assert.assertEquals(60, r.getShardIdForTime("20160301000000"));
        
        Assert.assertEquals("20160301000000", r.getShardDateString("20160301121300"));
    }
}
