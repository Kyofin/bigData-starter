package com.wugui.sparkstarter.demo1;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;

public class MockData {
    public static void main(String[] args) {
        StringBuffer sb = new StringBuffer();
        Random rand = new Random();
        List<String> device = new ArrayList<String>();
        for(int i=0;i<100;i++){
            device.add(getUUID());
        }
        for(int j=0;j<1000;j++){
            Calendar cal = Calendar.getInstance();
            cal.setTime(new Date());
            cal.add(Calendar.MINUTE,-rand.nextInt(6000));
            long timeStamp =cal.getTimeInMillis();
            String deviceId = device.get(rand.nextInt(100));

            long upTraffic = rand.nextInt(100000);
            long downTraffic= rand.nextInt(10000);
            sb.append(timeStamp).append("\t").append(deviceId).append("\t").append(upTraffic).append("\t").append(downTraffic).append("\n");
        }
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("./app_log.txt")));
            pw.write(sb.toString());
        } catch (FileNotFoundException e) {

        }finally {
            pw.close();
        }

    }
    public static  String getUUID(){
        return UUID.randomUUID().toString().replace("-","");
    }
}