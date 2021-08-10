package com.zdjizhi.common;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.HashSet;

public class DosSketchLog implements Serializable {

    private String common_sled_ip;
    private String common_data_center;
    private long sketch_start_time;
    private long sketch_duration;
    private String attack_type;
    private String source_ip;
    private String destination_ip;
    private long sketch_sessions;
    private long sketch_packets;
    private long sketch_bytes;
    private HashSet<String> source_ip_list;

    public HashSet<String> getSource_ip_list() {
        return source_ip_list;
    }

    public void setSource_ip_list(HashSet<String> source_ip_list) {
        this.source_ip_list = source_ip_list;
    }

    @Override
    public String toString() {
        return "DosSketchLog{" +
                "common_sled_ip='" + common_sled_ip + '\'' +
                ", common_data_center='" + common_data_center + '\'' +
                ", sketch_start_time=" + sketch_start_time +
                ", sketch_duration=" + sketch_duration +
                ", attack_type='" + attack_type + '\'' +
                ", source_ip='" + source_ip + '\'' +
                ", destination_ip='" + destination_ip + '\'' +
                ", sketch_sessions=" + sketch_sessions +
                ", sketch_packets=" + sketch_packets +
                ", sketch_bytes=" + sketch_bytes +
                ", source_ip_list=" + StringUtils.join(source_ip_list, ",") +
                '}';
    }

    public String getCommon_sled_ip() {
        return common_sled_ip;
    }

    public void setCommon_sled_ip(String common_sled_ip) {
        this.common_sled_ip = common_sled_ip;
    }

    public String getCommon_data_center() {
        return common_data_center;
    }

    public void setCommon_data_center(String common_data_center) {
        this.common_data_center = common_data_center;
    }

    public long getSketch_start_time() {
        return sketch_start_time;
    }

    public void setSketch_start_time(long sketch_start_time) {
        this.sketch_start_time = sketch_start_time;
    }

    public long getSketch_duration() {
        return sketch_duration;
    }

    public void setSketch_duration(long sketch_duration) {
        this.sketch_duration = sketch_duration;
    }

    public String getAttack_type() {
        return attack_type;
    }

    public void setAttack_type(String attack_type) {
        this.attack_type = attack_type;
    }

    public String getSource_ip() {
        return source_ip;
    }

    public void setSource_ip(String source_ip) {
        this.source_ip = source_ip;
    }

    public String getDestination_ip() {
        return destination_ip;
    }

    public void setDestination_ip(String destination_ip) {
        this.destination_ip = destination_ip;
    }

    public long getSketch_sessions() {
        return sketch_sessions;
    }

    public void setSketch_sessions(long sketch_sessions) {
        this.sketch_sessions = sketch_sessions;
    }

    public long getSketch_packets() {
        return sketch_packets;
    }

    public void setSketch_packets(long sketch_packets) {
        this.sketch_packets = sketch_packets;
    }

    public long getSketch_bytes() {
        return sketch_bytes;
    }

    public void setSketch_bytes(long sketch_bytes) {
        this.sketch_bytes = sketch_bytes;
    }
}
