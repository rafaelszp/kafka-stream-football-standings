package com.acme.footballstandings.serdes;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.util.Map;

public class JSONDeserializer<T> implements Deserializer<T> {

    private Class<T> destinationClass;
    private Type reflectionTypeToken;
    private Gson gson =
            new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();


    public JSONDeserializer(Class<T> tClass) {
        this.destinationClass = tClass;
    }

    public JSONDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        Type type = destinationClass != null ? destinationClass : reflectionTypeToken;
        return gson.fromJson(new String(bytes), type);
    }
}
