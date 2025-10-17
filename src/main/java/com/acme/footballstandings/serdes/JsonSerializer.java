package com.acme.footballstandings.serdes;

import com.acme.footballstandings.util.GsonFactory;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    Gson gson = GsonFactory.build();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }
}
