package com.wugui.sparkstarter.demo1;

import lombok.Data;
import scala.math.Ordered;

import java.io.Serializable;

/**
 * 根据key进行二次的排序，先按上行流量，在按下行流量，都一样的话安装时间戳排序
 */
@Data
public class AccessLogSortKey implements Ordered<AccessLogSortKey>,Serializable{
    //定义排序的三个对象
    private long timestamp;
    private long upTraffic;
    private long downTraffic;



    public AccessLogSortKey(long timestamp, long upTraffic, long downTraffic) {
        this.timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AccessLogSortKey that = (AccessLogSortKey) o;

        if (timestamp != that.timestamp) return false;
        if (upTraffic != that.upTraffic) return false;
        return downTraffic == that.downTraffic;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (upTraffic ^ (upTraffic >>> 32));
        result = 31 * result + (int) (downTraffic ^ (downTraffic >>> 32));
        return result;
    }



    private static  final  long  serivaVersionUID = 1L;
    @Override
    public int compare(AccessLogSortKey other) {
        if(upTraffic - other.upTraffic !=0){
            return (int)(upTraffic - other.upTraffic);
        }else if (downTraffic - other.downTraffic !=0){
            return (int)(downTraffic - other.downTraffic );
        }else if(timestamp -other.timestamp!=0){
            return (int)(timestamp -other.timestamp);
        }
        return 0;
    }

    @Override
    public int compareTo(AccessLogSortKey other) {
        if(upTraffic - other.upTraffic !=0){
            return (int)(upTraffic - other.upTraffic);
        }else if (downTraffic - other.downTraffic !=0){
            return (int)(downTraffic - other.downTraffic );
        }else if(timestamp -other.timestamp!=0){
            return (int)(timestamp -other.timestamp);
        }
        return 0;
    }

    @Override
    public boolean $less(AccessLogSortKey other) {
        if(upTraffic<other.upTraffic){
            return true;
        }else if(upTraffic==other.upTraffic&&downTraffic<other.downTraffic){
            return true;
        }else if(upTraffic==other.upTraffic&&downTraffic==other.downTraffic&&timestamp<other.timestamp){
            return true;
        }
        return false;
    }

    //这个是大于的方法
    @Override
    public boolean $greater(AccessLogSortKey other) {
        if(upTraffic>other.upTraffic){
            return true;
        }else if(upTraffic==other.upTraffic&&downTraffic>other.downTraffic){
            return true;
        }else if(upTraffic==other.upTraffic&&downTraffic==other.downTraffic&&timestamp>other.timestamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(AccessLogSortKey other) {
        if($less(other)){
            return true;
        }else if (upTraffic==other.upTraffic&&downTraffic==other.downTraffic&&timestamp==other.timestamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(AccessLogSortKey other) {
         if($greater(other)){
            return true;
        }else if (upTraffic==other.upTraffic&&downTraffic==other.downTraffic&&timestamp==other.timestamp){
             return true;
         }
         return false;
    }


}






















