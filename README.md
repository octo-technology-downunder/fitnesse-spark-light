# fitnesse-spark-light
Execute Spark Job from Fitnesse tests

## Fixtures

A fixture is a way to enrich the capabilities of Fitnesses. 

This project focus on 2 main features:
- [ExecuteSparkJob](https://github.com/octo-technology-downunder/fitnesse-spark-light/blob/master/src/main/java/au/com/octo/fitnesse/fixtures/ExecuteSparkJob.java) fixture: to run spark jobs
- [CsvFile](https://github.com/octo-technology-downunder/fitnesse-spark-light/blob/master/src/main/java/au/com/octo/fitnesse/fixtures/CsvFile.java) fixture: a way to read and write CSV

Using those fixture you will be able to create and run tests:
- reading a fitnesses table and convert it to an spark view using the the CsvFile fixture.
- executing a spark job with a few basics customizations, using the ExecuteSparkJob fixture.
- use a fitnesse table as expected output of the spark job, using the same CsvFile fixture.

## sample of a working fitnesse test using those fixture

Send us a message :)

