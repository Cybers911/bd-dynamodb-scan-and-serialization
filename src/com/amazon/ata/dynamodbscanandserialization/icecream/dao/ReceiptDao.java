package com.amazon.ata.dynamodbscanandserialization.icecream.dao;

import com.amazon.ata.dynamodbscanandserialization.icecream.converter.ZonedDateTimeConverter;
import com.amazon.ata.dynamodbscanandserialization.icecream.model.Receipt;
import com.amazon.ata.dynamodbscanandserialization.icecream.model.Sundae;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides access to receipts in the datastore.
 */
public class ReceiptDao {

    private final ZonedDateTimeConverter converter;
    private final DynamoDBMapper mapper;

    /**
     * Constructs a DAO with the given mapper.
     * @param mapper The DynamoDBMapper to use
     */
    @Inject
    public ReceiptDao(DynamoDBMapper mapper) {
        this.mapper = mapper;
        this.converter = new ZonedDateTimeConverter();
    }

    /**
     * Generates and persists a customer receipt. The salesTotal is the sum of the price of the
     * provided sundaes.
     * @param customerId - the id of the ordering customer
     * @param sundaeList - the sundaes ordered by the customer
     * @return the receipt stored in the database
     */
    public Receipt createCustomerReceipt(String customerId, List<Sundae> sundaeList) {
        Receipt receipt = new Receipt();
        receipt.setCustomerId(customerId);
        receipt.setPurchaseDate(ZonedDateTime.now());
        receipt.setSalesTotal(sundaeList.stream().map(Sundae::getPrice).reduce(BigDecimal.ZERO, BigDecimal::add));
        receipt.setSundaes(sundaeList);
        mapper.save(receipt);
        return receipt;
    }

    /**
     * Calculates the total sales for the time period between fromDate and toDate (inclusive).
     * @param fromDate - the date (inclusive of) to start tracking sales
     * @param toDate - the date (inclusive of) to stop tracking sales
     * @return the total values of sundae sales for the requested time period
     */
    public BigDecimal getSalesBetweenDates(ZonedDateTime fromDate, ZonedDateTime toDate) {
        //we want to create a map to hold the start date and the end date
        //build a scan expression with a filter between the start and end dates using the values
        //from the hashmap
        //return the resulting stream mapping over each sales to get the total sales and reduce this
        //to be a big single decimal value

        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":startDate", new AttributeValue().withS(converter.convert(fromDate)));// este codigo
        // es para convertir ZonedDateTime a String usando el AttributeValue para que
        // sea compatible con DynamoDB
        valueMap.put(":endDate", new AttributeValue().withS(converter.convert(toDate)));
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("purchaseDate BETWEEN :startDate AND :endDate")
                .withExpressionAttributeValues(":startDate", valueMap)
                .withExpressionAttributeValues(":endDate", valueMap);

        PaginatedScanList<Receipt> result = mapper.scan(Receipt.class, scanExpression);

        return result.stream()
                .map(Receipt::getSalesTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        //aca se deben devolver los receipts que se encuentren en el rango de fechas
        // y se reducen los valores de salesTotal de los receipts para obtener el total de ventas
        //
        // este metodo deberia devolver una lista de Receipts que se encuentren en el rango de fechas
        // y se reducen los valores de salesTotal de los receipts para obtener el total de ventas
        //
        // esta implementacion no se ha realizado ya que esta clase es un ejemplo y no se ha implementado
        // la logica para interactuar con DynamoDB

    }

    /**
     * Retrieves a subset of the receipts stored in the database. At least limit number of records will be retrieved
     * unless the end of the table has been reached, and instead only the remaining records will be returned. An
     * exclusive start key can be provided to start reading the table from this record, but excluding it from results.
     * @param limit - the number of Receipts to return
     * @param exclusiveStartKey - an optional value provided to designate the start of the read
     * @return a list of Receipts
     */
    public List<Receipt> getReceiptsPaginated(int limit, Receipt exclusiveStartKey) {
        //build a scann expression with a limit and an optional exclusive start key
        //if there is an exclusive starting key then
        //create a starting key map to hold the customer id and the purchased date

        //set the scann expression exclusive starting key

        //create a receiptPage by using the mapper scanPage method passing in the clss
        //scan expression and the limit

        // return the results from the receiptPage

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withLimit(limit);

        if (exclusiveStartKey!= null) {
            Map<String, AttributeValue> startKeyMap = new HashMap<>();
            startKeyMap.put(":customerId", new AttributeValue().withS(exclusiveStartKey.getCustomerId()));
            startKeyMap.put(":purchaseDate", new AttributeValue().withS(converter.convert(exclusiveStartKey.getPurchaseDate())));
            scanExpression.withExclusiveStartKey(startKeyMap);
        }//Este cosigo loque hace es construir un scanExpression con limit y un exclusiveStartKey para que
        // la lectura comience desde un registro determinado, pero excluyendolo de los resultados
        ScanResultPage<Receipt> receiptPage = mapper.scanPage(Receipt.class, scanExpression);// este cosigo lo que hace es crear una
         // PaginatedScanList de Receipts que comienza desde el exclusiveStartKey utilizando el scanExpression
        // y el limit , el ScanResultPage se devuelve como resultado y el <Receipt> se utiliza para
        // usar el DynamoDBMapper para mapear el resultado a un Receipt

        // si el exclusiveStartKey es null entonces devuelve los primeros limit Receipts


        // si el exclusiveStartKey es distinto de null entonces devuelve los resultados de la PaginatedScanList de Receipts
        // a partir del exclusiveStartKey


        // devuelve los resultados de la PaginatedScanList de Receipts


        // si no hay un exclusiveStartKey entonces devuelve los primeros limit Receipts


        // si hay un exclusiveStartKey entonces devuelve los resultados de la PaginatedScanList de Receipts
        // a partir del exclusiveStartKey


        // devuelve los resultados de la PaginatedScanList de Receipts
        return receiptPage.getResults();
    }
}
