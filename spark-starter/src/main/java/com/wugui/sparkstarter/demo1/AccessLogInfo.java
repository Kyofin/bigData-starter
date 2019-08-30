package com.wugui.sparkstarter.demo1;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;


//log日志的javaBean
@Data
@AllArgsConstructor
public class AccessLogInfo implements Serializable {

    private static final  long  serivaVersionUID = 1L;
    private long  Timestamp;
    private  long upTraffic;
    private  long downTraffic;



}