## Phase 1: Long live the sundaes!

In this phase, we'll be working on persisting the sundae order in the receipts. Each order at the ice cream
parlor can contain multiple sundaes. The `Receipt` model does not currently have a field for the sundaes that
were ordered, just the total purchase amount. ****We will need to add a field called `sundaes` to the class, and then,
like the `ZonedDateTime`, create a conversion class for DynamoDB to use when reading and writing to the table. We would
like to save the ordered sundaes as JSON. If an exception occurs while serializing/deserializing you can throw a
`SundaeSerializationException` (already defined).****

When serializing, the mapper can determine the class of each object in the list.
When deserializing, there's no way to determine what Java class a `String` might represents,
so we have to pass a parameter to the `readValue` method.
Unfortunately, generic parameters *don't have a type*: they're compiler hints, not actual code.
Jackson provides `TypeReference` to specify genericized classes.

When updating the `Receipt` model, we will **not** be editing the `hashcode` and `equals` methods.
However, you will need to remove the `@DynamoDBIgnore` annotation from the `getSundaes` method.

Consider the following question, then implement your solution:
* What happens to the items already in the datastore?

GOAL: Update `Receipt` and `ReceiptDao` to include the sundae order.

Phase 1 is complete when:
- `serialization/Phase1Test` passes