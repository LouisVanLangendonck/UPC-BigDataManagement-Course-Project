# UPC-BigDataManagement-Course-Project
Course Project for Big Data Management Course 2022 at the Politechnical University of Catalunya (UPC). Authors are Louis Van Langendonck (part 1 and 2), Pim Schoolkate (part 1) and Alex Martorell (part 2). For each of both parts of the project, a detailed report can be found in the repository under the name of respectively 01_report.pdf and 02_report.pdf.

## Project Statement
The goal of the project is managing and analyzing different Barcelona Housing data sources to emulate a complete big data pipeline. 

The first part captures the landing zone, further divided into temporal and persistent stages. In this part, different big data storage techniques are explored and compared (HDFS with different Fragmentation Strategies, Hbase, MongoDB, ...)

The second part considers the formatted zone and the exploitation zone. Both processes rely heavily on Spark and its various functionalities. The former uses spark transformations for data cleaning, integration and reconciliation. The latter contains both descriptive and predictive analysis. The descriptive analysis makes more use of simple spark transformations to satisy three KPI's that are visualized using Tableau. The predictive analysis uses spark ML to train a model and Spark Structured Streaming to ingest a stream and subsequently apply the pre-trained model.

## How To Run
Each of the reports starts with a detailed account on how to run that part of the project. 
