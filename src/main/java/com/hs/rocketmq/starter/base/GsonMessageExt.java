package com.hs.rocketmq.starter.base;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @Description:
 * @Author: HS
 * @Date: 2019/5/5 15:20
 */
public class GsonMessageExt {
    private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    public static Gson getJson(){
        gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        return gson;
    }
}
