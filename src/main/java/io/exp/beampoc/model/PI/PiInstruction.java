package io.exp.beampoc.model.PI;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PiInstruction implements  Serializable{
     String id;
    public String SeriesName;
    public int numOfSteps;

    public PiInstruction(){
        id = UUID.randomUUID().toString();
    }

    @Override
    public String toString() {


        Gson gson = new Gson();

        String jsonString = gson.toJson(this);


        return jsonString;
    }
    public static PiInstruction fromJson(String json){
        Gson gson = new Gson();
        PiInstruction i=gson.fromJson(json,PiInstruction.class);
        return i;
    }
/*
    public Map<String, Serializable> toMap(){
        Map<String, Serializable> m = new HashMap<String, Serializable>();
        m.put("id",this.id);
        m.put("SeriesName",this.SeriesName);
        m.put("numOfSteps",this.numOfSteps);
        return m;
    }*/
    /*
    public static PiInstruction fromMap(Map<String, Serializable> m){

        return null;
    }*/
}
