package com.moap.dataprocess.service;

import com.alibaba.fastjson.JSONObject;
import com.hyt.common.NormalData;

import java.util.List;

public interface ProcessService {

    /**
     * 将数据文件整合成大文件，并且生成索引文件
     */
    public void generateDataFile();

    /**
     * 获取所有时间数据集合
     * @return
     */
    public List<String> getTimeList();


    /**
     * 根据请求获取normaldata
     * @param url
     * @return
     */
    public NormalData getNormalData(String url);


}
