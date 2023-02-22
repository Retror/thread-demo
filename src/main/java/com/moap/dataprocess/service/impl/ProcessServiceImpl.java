package com.moap.dataprocess.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyt.common.NormalData;
import com.moap.dataprocess.service.ProcessService;
import com.moap.dataprocess.util.HttpUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class ProcessServiceImpl implements ProcessService {

    @Override
    public void generateDataFile() {

    }

    @Override
    public List<String> getTimeList() {
        String url = "http://10.1.64.146/mdfs/v1.1/meta/list?dataPath=ECMWF_HR/HGT/500&type=mdfs&initTime=23021508";
        List<String> result = new ArrayList<>();
        try {
            String str = HttpUtils.get(url);
            str = str.substring(1,str.length()-1);
            result = Arrays.asList(str.split(","));
        } catch (IOException e) {
            return result;
        }
        return result;
    }

    @Override
    public NormalData getNormalData(String url) {
        NormalData normalData = new NormalData();
        try {
            String s = HttpUtils.get(url);
            ObjectMapper objectMapper = new ObjectMapper();
            normalData = objectMapper.readValue(s, NormalData.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return normalData;
    }
}
