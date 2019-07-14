package io.exp.beampoc.stream.PI.Model;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.UUID;

public class PiInstruction implements  Serializable{
    public  String id;
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

}
