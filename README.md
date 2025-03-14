# PySpark Hashtag Counter  

## Overview  
This project involves a comprehensive analysis of a dataset containing tweets, aiming to **identify the top 20 most prevalent hashtags**. The data is hosted on an **AWS S3 bucket**, demonstrating seamless **integration with cloud storage solutions** for scalable data processing.  

---

## Data Processing with Apache Spark 
To efficiently handle and analyze the dataset, we leveraged **Apache Spark**, a powerful big data processing framework known for its **distributed computing and high-speed analytics**.  

### Why Apache Spark?  
- **Handles large-scale data efficiently** with distributed computing.  
- **Supports parallel processing**, making hashtag extraction and aggregation faster.  
- **Optimized for cloud environments** like AWS EMR, ensuring scalability.  

---

## Custom Map-Reduce Implementation  
The **core logic** of our hashtag analysis resides in the **`hashtag_code.py`** script, which implements a **custom Map-Reduce approach** to process tweets in parallel.  

### How It Works? 
1. **Mapper Phase** → Extracts hashtags from each tweet and assigns a count of **1** to each occurrence.  
2. **Reducer Phase** → Aggregates the counts for each hashtag across the entire dataset.  
3. **Final Sorting** → Retrieves the **top 20 most frequent hashtags**.  

This Map-Reduce model allows for **efficient hashtag aggregation across large datasets** while taking advantage of Spark's **parallel execution model**.  

---

## Deployment on Amazon EMR 
To **scale up data processing**, the project is deployed on **Amazon Elastic MapReduce (EMR)**, an AWS-managed service that simplifies running **big data frameworks** like Apache Spark.  

### Why Amazon EMR?  
- **Auto-Scalability** – Dynamically adjusts compute resources for optimal performance.  
- **Distributed Data Processing** – Speeds up execution time by leveraging multiple nodes.  
- **Seamless AWS Integration** – Easily connects with **S3, IAM roles, and CloudWatch** for monitoring.  

### Deployment Steps on EMR  
1. **Launch an Amazon EMR cluster** with Spark enabled.  
2. **Upload the dataset to an AWS S3 bucket**.  
3. **Run `hashtag_code.py` on the EMR cluster**, processing the data in parallel.  
4. **Store the output in a specified S3 bucket folder** for easy access.  

---

## Output Storage & Accessibility 
After the **data processing is complete**, the results are **automatically stored in an Amazon S3 bucket**, ensuring **secure and scalable storage**.  

- **Output Format**: A structured file containing the **top 20 most-used hashtags** and their respective counts.  
- **Location**: Stored in a designated folder in **AWS S3**, making it accessible for further analysis or visualization.  

---

## Technologies Used  
- **Programming Language**: Python (PySpark)  
- **Big Data Framework**: Apache Spark  
- **Cloud Platform**: AWS (EMR, S3)  
- **Data Processing Model**: Map-Reduce  
- **Storage**: AWS S3  

---
