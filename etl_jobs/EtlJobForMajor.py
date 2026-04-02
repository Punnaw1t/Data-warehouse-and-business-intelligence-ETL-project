import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, DateType, ArrayType
from pyspark.sql import functions as pysparkFunctions, Window


class EtlJobForMajor:

    def __init__(self, **kwargs):
        self.spark_session = (
            SparkSession.builder 
            .master(os.getenv('SPARK_MASTER_URL', 'local[*]')) 
            .appName("SparkAppForEtlJob1")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") 
            .getOrCreate()
        )
        
        self.input_file = kwargs['source']
        self.log = logging.getLogger(__name__)
        
        if (kwargs.get('database')):
            self.database_name = kwargs['database']
            self.table_name = kwargs['table']
            self.load_type = "database"
    
        elif (kwargs.get('destination')):
            self.output = kwargs['destination']
            self.load_type = "parquet"
    

    def extract_transactions(self):
        schema = StructType([
            StructField("transactionId", StringType(), True),
            StructField("custId", StringType(), True),
            StructField("transactionDate", DateType(), True),
            StructField("productSold", StringType(), True),
            StructField("unitsSold", IntegerType(), True)
            ])
        df_transactions = self.spark_session.read.schema(schema).csv(
                self.input_file,
                sep='|',
                header=True,
                escape="\"")
        return df_transactions

    def transform(self, df_transactions):
        df_transactions.createOrReplaceTempView('view_transactions')

        total_transactions = self.spark_session.sql("""
            SELECT * from view_transactions
            """).count()
        unique_customers = self.spark_session.sql("""
            SELECT custId from view_transactions
                group by custId
            """).count()
        self.log.info('''
        SUMMARY:
            Total Transactions: %s
            Total Customers   : %s
        ''', total_transactions, unique_customers)

        self.log.info('''TASK:
                    Calculating the favorites
        ''')
        df_favorites = self.spark_session.sql("""
            WITH a as (
                SELECT * FROM (SELECT
                    custId       as custId,
                    productSold as productSold,
                    sum(vt.unitsSold) as totalUnitsSold,
                    RANK() OVER (partition by custId ORDER BY sum(vt.unitsSold) desc) AS ranking
                  FROM view_transactions vt
                  GROUP BY custId,productSold
                  )
                  where ranking = 1
            )
            SELECT custId, productSold, count(custId) as cnt from a GROUP BY custId,productSold ORDER BY cnt DESC
            -- SELECT custId, count(*) as cnt from a GROUP BY custId ORDER BY cnt DESC
        """)
        return df_favorites

    def load(self, load_type, df_to_load):
        if (load_type == 'parquet'):
            df_to_load.write.mode("overwrite").parquet(self.output)
            self.log.info(
                    'Data successfully written on the path: %s',
                    self.output)
        if (load_type == 'database'):
            #*********************** pass ******************************
            self.log.info("Loading data into Postgres database: %s, table: %s", self.database_name, self.table_name)
            
            db_host = os.getenv('POSTGRES_HOST', 'db')
            db_port = os.getenv('POSTGRES_PORT', '5432')
            db_user = os.getenv('POSTGRES_USER', 'user') 
            db_password = os.getenv('POSTGRES_PASSWORD', 'password')
            
            url = f"jdbc:postgresql://{db_host}:{db_port}/{self.database_name}"
            
            properties = {
                "user": db_user,
                "password": db_password,
                "driver": "org.postgresql.Driver"
            }
            
            df_to_load.write.jdbc(url=url, table=self.table_name, mode="overwrite", properties=properties)
            self.log.info("Data successfully loaded to database")
            
            #****************************************           
            
        if (load_type not in ['parquet', 'database']):
            self.log.info("The current output format is yet not supported")

    def run(self):
        self.log.info('Kicking off the etl job... ')
        return self.load(self.load_type,
                self.transform(
                    self.extract_transactions(),
                    ))
