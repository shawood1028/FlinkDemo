package com.shawood.ch05;

import java.sql.Timestamp;

/**
 * 该类用于: 模拟网站访问数据
 *
 * @author shawood
 * @version 1.0
 * @date 2022-04-27
 */
public class Event {
    public String url;
    public String user;
    public Long timestamp;

    public Event(){
    }

    public Event(String user, String url, Long timestamp){
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
