from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
schema = 'Age INTEGER, Sex STRING, ChestPainType STRING RestingBp INTEGER Cholesterol INTEGER FastingBS INTEGER RestingECG STRING MaxHR INTEGER ExerciseAngina STRING Oldpeak INTEGER ST_Slope STRING HeartDisease INTEGER  '
#df = spark.read.csv('../data/heart.csv', inferschema=True,header=True)
df = spark.read.csv('../data/heart.csv', schema=schema,header=True)
#df.write.format("csv").save("../data/heart_save.csv")
#df.write.format("csv").mode("overwrite").save("../data/heart_save.csv")
#df.printSchema()

#from pyspark.sql.types import FloatType
#df = df.withColumn("Age",df.Age.cast(FloatType()))
#df = df.withColumn("RestingBP",df.Age.cast(FloatType()))

# compute summery statistics
#df.select(['Age','RestingBP']).describe().show()

df = df.na.drop()
df = df.na.drop(how='all')
df = df.na.drop(thresh=2)
df = df.na.drop(how='any', subset=['age','sex']) 
df = df.na.fill(value='?',subset=['sex'])
from pyspark.ml.feature import Imputer 
imptr = Imputer(inputCols=['age','RestingBP'],
                outputCols=['age','RestingBP']).setStrategy('mean') 
df = imptr.fit(df).transform(df)


df.filter('age> 18').show()

df.filter('age > 18')
df.where('age > 18')
df.where(df['age'] > 18) 
df.where((df['age'] > 18) | (df['ChestPainType'] == 'ATA'))
df.filter(~(df['ChestPainType'] == 'ATA'))

from pyspark.sql.functions import expr
exp = 'age + 0.2 * AgeFixed'
df.withColumn('new_col', expr(exp)).select('new_col').show(3)


disease_by_age = df.groupby('age').mean().select(['age','avg(HeartDisease)'])
from pyspark.sql.functions import desc
disease_by_age.orderBy(desc("age")).show(5)
from pyspark.sql.functions import asc
disease_by_age = df.groupby('age').mean().select(['age','avg(HeartDisease)'])
disease_by_age.orderBy(asc("age")).show(3)

from pyspark.sql import functions as F
df.agg(F.min(df['age']),F.max(df['age']),F.avg(df['sex'])).show()

df.groupby('HeartDisease').agg(F.min(df['age']),F.avg(df['sex'])).show()

df.createOrReplaceTempView("df") 
spark.sql("""SELECT sex from df""").show(2)

df.selectExpr("age >= 40 as older", "age").show(2)

df.groupby('age').pivot('sex', ("M", "F")).count().show(3)

df.selectExpr("age >= 40 as older", "age",'sex').groupBy("sex")\
                    .pivot("older", ("true", "false")).count().show()

df.select(['age','MaxHR','Cholesterol']).show(4)

input("Please enter CTRL + C")
