package com.acme.footballstandings.serdes;

public class SerdeFactory<T> {

    public JSONSerdes<T> createSerde(Class<T> clazz){
        return new JSONSerdes<>(clazz);
    }

}

