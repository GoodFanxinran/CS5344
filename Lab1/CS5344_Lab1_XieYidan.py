'''
 *@author: XieYidan
 *@date: 06/02/2022
 *@description: The code is used to find the top 15 products based on the number of user reviews
 and report their average rating and product price.
 *@version information:
    -MacOS (64-bit)
    -java 1.8.0_321
    -Spark 3.2.0
    -Scala 2.12.15
    -Python 3.8.12
    -pyspark 4.10.3
'''


# Import the necessary libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys


if __name__ == '__main__':

    conf = SparkConf().setAppName("A0225651J_Lab1")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    #1. Read "meta_Baby.json" and "review_Baby.json"
    rdd_meta = sqlContext.read.json(sys.argv[1]).rdd
    rdd_review = sqlContext.read.json(sys.argv[2]).rdd

    #2. Keep 'asin' and 'overall' in data (reviews_Baby)
    rdd_review_num = rdd_review.map(lambda x: (x['asin'], 1))
    rdd_review_rating = rdd_review.map(lambda x: (x['asin'], x['overall']))

    #3. Perform some operations for data
    '''
    Step 1. Find the number of reviews and calculate the average rating for each product from the review file. Use pair 
    RDD, reduceByKey and map function to accomplish this step. The key is the product ID/asin.
    '''

    # (1) The number of reviews
    review_num = rdd_review_num.reduceByKey(lambda a, b: a + b)


    # (2) The average rating for each product
    review_rating = rdd_review_rating.reduceByKey(lambda a, b: a + b)

    # Join these two RDD ('review_num' and 'review_rating') to calculate the average rating
    review_num_rating = review_num.join(review_rating)

    #Calculate the average rating
    review_rating_aver = review_num_rating.map(lambda x: (x[0], (x[1][0], x[1][1]/x[1][0])))


    '''
    Step 2. Create an RDD where the key is the product ID/asin and value is the price of the product. Use the metadata 
    for this step.
    '''
    rdd_meta_price = rdd_meta.map(lambda x: (x['asin'], x['price']))

    '''
    Step 3. Join the pair RDD obtained in Step 1 and the RDD created in Step 2.
    '''
    rdd_rating_num_price = review_rating_aver.join(rdd_meta_price)

    '''
    Step 4. Find the top 15 products with the greatest number of reviews.
    '''
    review_num_order = rdd_rating_num_price.sortBy(lambda x: x[1][0][0], ascending=False, numPartitions=1)
    top15 = review_num_order.take(15)

    '''
    Step 5. Output the average rating and price for the top 15 products identified in Step 4.
    '''
    #Obtain (product asin, average rating, price) for the top 15 products
    top15_rating_aver_price = review_num_order.map(lambda x: (x[0], x[1][0][1], x[1][1])).take(15)
    #Transform 'list' into 'RDD'
    top15_rating_aver_price_rdd = sc.parallelize(top15_rating_aver_price)
    #Save RDD into the output file
    top15_rating_aver_price_rdd.coalesce(1).map(lambda row: str(row)).saveAsTextFile(sys.argv[3])

    sc.stop()