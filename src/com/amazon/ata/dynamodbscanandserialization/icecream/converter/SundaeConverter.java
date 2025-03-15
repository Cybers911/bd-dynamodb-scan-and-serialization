package com.amazon.ata.dynamodbscanandserialization.icecream.converter;

import com.amazon.ata.dynamodbscanandserialization.icecream.exception.SundaeSerializationException;
import com.amazon.ata.dynamodbscanandserialization.icecream.model.Sundae;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

import static org.junit.platform.commons.util.StringUtils.isBlank;

public class SundaeConverter implements DynamoDBTypeConverter<String, List<Sundae>> {// Lo que hicimos aqhi es agregar una
    // lista de Sundaes en lugar de un solo Sundae para convertir el string a una lista de Sundaes
    private ObjectMapper mapper;// Inicializamos un ObjectMapper para mapear JSON a objetos y viceversa
    //Constructor
    public SundaeConverter() {
        mapper = new ObjectMapper();// Inicializamos el ObjectMapper para mapear JSON a objetos y viceversa
    }
    //Método para convertir una lista de Sundaes a un String (JSON) y viceversa hacia una lista de Sundaes
    public String convert(List<Sundae> sundaes) {//convierte una lista a cadena JSON
        if (sundaes == null || sundaes.isEmpty()) {
            return "";
        }
        String jsonSundaes;
        try {
            jsonSundaes = mapper.writeValueAsString(sundaes);
        } catch (JsonProcessingException e) {
        throw new SundaeSerializationException(e.getMessage(), e);
        }

        return jsonSundaes;
    }
    public List<Sundae> unconvert(String jsonSundaes) {//convierte una cadena JSON a una lista de Sundaes}) {
            //convierte una cadena JSON a una lista de Sundaes
        // }
        List<Sundae> sundaes = new ArrayList<>();
        if (isBlank(jsonSundaes)) {
            return sundaes;

        }
        try {
            sundaes = mapper.readValue(jsonSundaes, new TypeReference<List<Sundae>>() {});// Deserializa la cadena JSON a una lista de
                // Sundaes y la devuelve en la lista sundaes lo que hace TypeReference para especificar el tipo de lista de Sundaes que se

        } catch (JsonProcessingException e) {
            throw new SundaeSerializationException(e.getMessage(), e);
        }
        return sundaes;


    }
}
