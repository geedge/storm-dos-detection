package com.zdjizhi.common;

import java.io.Serializable;

public class DosEventLog implements Serializable {

    private long log_id;
    private long start_time;
    private long end_time;
    private String attack_type;
    private String severity;
    private String conditions;
    private String destination_ip;
    private String destination_country;
    private String source_ip_list;
    private String source_country_list;
    private long session_rate;
    private long packet_rate;
    private long bit_rate;

    public long getLog_id() {
        return log_id;
    }

    public void setLog_id(long log_id) {
        this.log_id = log_id;
    }

    public long getStart_time() {
        return start_time;
    }

    public void setStart_time(long start_time) {
        this.start_time = start_time;
    }

    public long getEnd_time() {
        return end_time;
    }

    public void setEnd_time(long end_time) {
        this.end_time = end_time;
    }

    public String getAttack_type() {
        return attack_type;
    }

    public void setAttack_type(String attack_type) {
        this.attack_type = attack_type;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getConditions() {
        return conditions;
    }

    public void setConditions(String conditions) {
        this.conditions = conditions;
    }

    public String getDestination_ip() {
        return destination_ip;
    }

    public void setDestination_ip(String destination_ip) {
        this.destination_ip = destination_ip;
    }

    public String getDestination_country() {
        return destination_country;
    }

    public void setDestination_country(String destination_country) {
        this.destination_country = destination_country;
    }

    public String getSource_ip_list() {
        return source_ip_list;
    }

    public void setSource_ip_list(String source_ip_list) {
        this.source_ip_list = source_ip_list;
    }

    public String getSource_country_list() {
        return source_country_list;
    }

    public void setSource_country_list(String source_country_list) {
        this.source_country_list = source_country_list;
    }

    public long getSession_rate() {
        return session_rate;
    }

    public void setSession_rate(long session_rate) {
        this.session_rate = session_rate;
    }

    public long getPacket_rate() {
        return packet_rate;
    }

    public void setPacket_rate(long packet_rate) {
        this.packet_rate = packet_rate;
    }

    public long getBit_rate() {
        return bit_rate;
    }

    public void setBit_rate(long bit_rate) {
        this.bit_rate = bit_rate;
    }

    @Override
    public String toString() {
        return "dosEventLog{" +
                "log_id=" + log_id +
                ", start_time=" + start_time +
                ", end_time=" + end_time +
                ", attack_type='" + attack_type + '\'' +
                ", severity='" + severity + '\'' +
                ", conditions='" + conditions + '\'' +
                ", destination_ip='" + destination_ip + '\'' +
                ", destination_country='" + destination_country + '\'' +
                ", source_ip_list='" + source_ip_list + '\'' +
                ", source_country_list='" + source_country_list + '\'' +
                ", session_rate=" + session_rate +
                ", packet_rate=" + packet_rate +
                ", bit_rate=" + bit_rate +
                '}';
    }
}
