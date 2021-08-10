package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;

public class IpUtils {

    /**
     * IP定位库工具类
     */
    public static IpLookup ipLookup = new IpLookup.Builder(false)
//            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
//            .loadDataFileV6(CommonConfig.IP_MMDB_PATH + "ip_v6.mmdb")
            .loadDataFilePrivateV4(CommonConfig.IP_MMDB_PATH + "ip_private_v4.mmdb")
//            .loadDataFilePrivateV6(CommonConfig.IP_MMDB_PATH + "ip_private_v6.mmdb")
            .build();

    public static void main(String[] args) {
        System.out.println(ipLookup.countryLookup("8.8.8.8"));
    }



}
