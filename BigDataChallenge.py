pyspark.sql.functions import split, concat_ws, substring, avg, when, lit

# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession

spark = SparkSession.builder.appName('Challenge.com').getOrCreate()

#1

employeeColumn = ["emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date"]
employeeData = [  
    ("10001","1953-09-02","Georgi","Facello","M","1986-06-26"),  
    ("10002","1964-06-02","Bezalel","Simmel","F","1985-11-21"),  
    ("10003","1959-12-03","Parto","Bamford","M","1986-08-28"),  
    ("10004","1954-05-01","Chirstian","Koblick","M","1986-12-01"),  
    ("10005","1955-01-21","Kyoichi","Maliniak","M","1989-09-12")  
]

employee_rdd = spark.sparkContext.parallelize(employeeData)
employee_df = employee_rdd.toDF(employeeColumn)

jobColumn = ["emp_no", "title", "from_date" , "to_date"] 

jobData = [  
    ("10001","Senior Engineer","1986-06-26","9999-01-01"),  
    ("10002","Staff","1996-08-03","9999-01-01"),  
    ("10003","Senior Engineer","1995-12-03","9999-01-01"),  
    ("10004","Senior Engineer","1995-12-01","9999-01-01"),  
    ("10005","Senior Staff","1996-09-12","9999-01-01")  
]

job_rdd = spark.sparkContext.parallelize(jobData)
job_df = job_rdd.toDF(jobColumn)

salaryColumn = ["emp_no", "title", "from_date" , "to_date"]

salaryData = [  
    ("10001","66074","1988-06-25","1989-06-25"), 
    ("10001","62102","1987-06-26","1988-06-25"), 
    ("10001","60117","1986-06-26","1987-06-26"), 
    ("10002","72527","2001-08-02","9999-01-01"), 
    ("10002","71963","2000-08-02","2001-08-02"), 
    ("10002","69366","1999-08-03","2000-08-02"),  
    ("10003","43311","2001-12-01","9999-01-01"), 
    ("10003","43699","2000-12-01","2001-12-01"), 
    ("10003","43478","1999-12-02","2000-12-01"),  
    ("10004","74057","2001-11-27","9999-01-01"),
    ("10004","70698","2000-11-27","2001-11-27"), 
    ("10004","69722","1999-11-28","2000-11-27"),  
    ("10005","94692","2001-09-09","9999-01-01"),
    ("10005","91453","2000-09-09","2001-09-09"), 
    ("10005","90531","1999-09-10","2000-09-09")   
]

salary_rdd = spark.sparkContext.parallelize(salaryData)
salary_df = salary_rdd.toDF(salaryColumn)

#2

def format_columns(col_list, x_df):
    for element in col_list:
        temp = element
        temp = temp.replace("_", " ")
        temp = temp.upper()
        x_df.withColumnRenamed(element,temp)
    
format_columns(employeeColumn, employee_df)

format_columns(jobColumn, job_df)

format_columns(salaryColumn, salary_df)

#3
employee_df.withColumn("BIRTH DATE", concat_ws('.', split(employee_df["BIRTH DATE"]).getItem(2), split(employee_df["BIRTH DATE"]).getItem(1), split(employee_df["BIRTH DATE"]).getItem(0)))

#4

employee_df.withColumn("EMAIL", concat_ws('', substring('FIRST NAME', 1,2), employee_df['LAST NAME'], "@company.com"))

#5

job_salary_df = salary_df.join(jobColumn, salary_df["EMP NO"] == jobColumn["EMP NO"]).select(jobColumn["TITLE"],avg(salary_df["TITLE"]))

job_salary_df.show()

#6