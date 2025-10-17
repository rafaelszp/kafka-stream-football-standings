package com.acme.footballstandings.util;

public class GsonFactory {

    public static com.google.gson.Gson build() {
        return new com.google.gson.GsonBuilder()
                .setFieldNamingPolicy(com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }
}
