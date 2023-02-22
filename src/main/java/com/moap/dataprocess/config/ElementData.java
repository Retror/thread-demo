package com.moap.dataprocess.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "data")
public class ElementData {
//    private List lists;
//
//    public List getLists() {
//        return lists;
//    }
//
//    public void setLists(List lists) {
//        this.lists = lists;
//    }

//    private Model model;
//
//    public Model getModel() {
//        return model;
//    }
//
//    public void setModel(Model model) {
//        this.model = model;
//    }

//    private List<Model> model;
//
//    public List<Model> getModel() {
//        return model;
//    }
//
//    public void setModel(List<Model> model) {
//        this.model = model;
//    }

    private DataInfo[] dataInfos;

    public DataInfo[] getDataInfos() {
        return dataInfos;
    }

    public void setDataInfos(DataInfo[] dataInfos) {
        this.dataInfos = dataInfos;
    }
}
