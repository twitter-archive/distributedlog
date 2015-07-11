package com.twitter.distributedlog.net;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestTwitterDNSResolver {

    @Test
    public void testTwitterDNSResolverForRacks() {
        TwitterDNSResolver dnsResolver = new TwitterDNSResolverForRacks("");

        List<String> ipList = new ArrayList<String>();
        ipList.add("192.0.0.1");
        List<String> racks = dnsResolver.resolve(ipList);
        assertEquals(TwitterDNSResolverForRacks.DEFAULT_RACK, racks.get(0));

        List<String> unknownList = new ArrayList<String>();
        unknownList.add("unknown");
        racks = dnsResolver.resolve(unknownList);
        assertEquals(TwitterDNSResolverForRacks.DEFAULT_RACK, racks.get(0));

        List<String> atlaList = new ArrayList<String>();
        atlaList.add("atla-bmj-37-sr1.prod.twttr.net");
        racks = dnsResolver.resolve(atlaList);
        assertEquals("/atla/bmj", racks.get(0));

        List<String> smf1List = new ArrayList<String>();
        smf1List.add("smf1-bfk-24-sr1.prod.twitter.com");
        racks = dnsResolver.resolve(smf1List);
        assertEquals("/smf1/bfk", racks.get(0));
    }


    @Test
    public void testTwitterDNSResolverForRows() {
        TwitterDNSResolver dnsResolver = new TwitterDNSResolverForRows("");

        List<String> ipList = new ArrayList<String>();
        ipList.add("192.0.0.1");
        List<String> rows = dnsResolver.resolve(ipList);
        assertEquals(TwitterDNSResolverForRows.DEFAULT_ROW, rows.get(0));

        List<String> unknownList = new ArrayList<String>();
        unknownList.add("unknown");
        rows = dnsResolver.resolve(unknownList);
        assertEquals(TwitterDNSResolverForRows.DEFAULT_ROW, rows.get(0));

        List<String> atlaList = new ArrayList<String>();
        atlaList.add("atla-bmj-37-sr1.prod.twttr.net");
        rows = dnsResolver.resolve(atlaList);
        assertEquals("/atla/bm", rows.get(0));

        List<String> smf1List = new ArrayList<String>();
        smf1List.add("smf1-bfk-24-sr1.prod.twitter.com");
        rows = dnsResolver.resolve(smf1List);
        assertEquals("/smf1/bf", rows.get(0));
    }

    @Test
    public void testTwitterDNSResolverOverrides() {
        TwitterDNSResolver dnsResolver = new TwitterDNSResolverForRacks("atla-eap-37-sr1:atl3;atla-ean-27-sr1:atl3");

        List<String> atlaList = new ArrayList<String>();
        atlaList.add("atla-bmj-37-sr1.prod.twttr.net");
        List<String> racks = dnsResolver.resolve(atlaList);
        assertEquals("/atla/bmj", racks.get(0));

        List<String> atl3List = new ArrayList<String>();
        atl3List.add("atla-ean-27-sr1.prod.twitter.com");
        racks = dnsResolver.resolve(atl3List);
        assertEquals("/atl3/ean", racks.get(0));
    }
}
