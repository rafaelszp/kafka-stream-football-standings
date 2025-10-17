package com.acme.footballstandings.model;

import com.acme.footballstandings.util.GsonFactory;

public abstract  class AbstractModel {

    public String toJsonString(){
        return GsonFactory.build().toJson(this);
    }

}
