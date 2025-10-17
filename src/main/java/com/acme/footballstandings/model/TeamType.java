package com.acme.footballstandings.model;

public enum TeamType {

    ANAPOLINA("ASSOCIAÇÂO ATLÉTICA ANAPOLINA","AAA"),
    GOIANESIA("GOIANÉSIA ESPORTE CLUBE","GEC"),
    ANAPOLIS("ANÁPOLIS FUTEBOL CLUBE","AFC"),
    APARECIDENSE("ASSOCIAÇÃO ATLÉTICA APARECIDENSE", "AAP");

    private String fullName;
    private String acronym;

    private TeamType(String fullName, String acronym) {
        this.fullName = fullName;
        this.acronym = acronym;
    }

    public static TeamType getByName(String name){
        for(TeamType teamType : TeamType.values()){
            if(teamType.name().equals(name)){
                return teamType;
            }
        }
        return null;
    }

}
