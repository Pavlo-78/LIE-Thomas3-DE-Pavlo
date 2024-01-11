#!/usr/bin/env python
# coding: utf-8

# ## 1. Preprocessing

# Create Spark Session
import numpy as np
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DiabetesPrediction").getOrCreate()

# Load Data
data_path = r"diabetes.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Extra tools
def pv_shape(df):
    print(df.count(),'x', len(df.columns))

pv_shape(df)
df.describe().show()

# Clean Missing Values and Drop Unnecessary Columns
# Remove rows with missing values
df = df.na.drop()

# Drop unnecessary columns
# columns_to_drop = ["SkinThickness"]  # Add the unnecessary columns here
# df = df.drop(*columns_to_drop)
# column "SkinThickness" is the least important, it can be removed, but the results are noticeably worse

pv_shape(df)

# ## 2. Feature Engineering
# VectorAssembler
from pyspark.ml.feature import VectorAssembler

feature_columns = ["Pregnancies", "Glucose", "BloodPressure", "Insulin", "BMI", "DiabetesPedigreeFunction", "Age", "SkinThickness"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df = assembler.transform(df)

# Split Between Test and Train
(train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)

# # 3. Model Training

# Train Different Models
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier

# Logistic Regression
lr = LogisticRegression(labelCol="Outcome", featuresCol="features")
lr_model = lr.fit(train_data)

# Random Forest
# rf = RandomForestClassifier(labelCol="Outcome", featuresCol="features")
rf = RandomForestClassifier(labelCol="Outcome", featuresCol="features", numTrees=200, maxDepth=5, featureSubsetStrategy='sqrt')
rf_model = rf.fit(train_data)

# Decision Tree
dt = DecisionTreeClassifier(labelCol="Outcome", featuresCol="features")
dt_model = dt.fit(train_data)


# # 4. Evaluation

# Function for evaluating Models
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def evaluate_model(model, data):
    t,r = '',''
    predictions = model.transform(data)
    model_name = str(model).split(":")[0]
    print("-"*60)
    
    # Evaluate accuracy
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="Outcome", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator_acc.evaluate(predictions)
    r = f"{model_name} Accuracy: {accuracy}"
    print(r)
    t += r
    
    # Evaluate precision
    true_positive = predictions.filter((predictions["Outcome"] == 1) & (predictions["prediction"] == 1)).count()
    false_positive = predictions.filter((predictions["Outcome"] == 0) & (predictions["prediction"] == 1)).count()    
    precision = true_positive / (true_positive + false_positive) if (true_positive + false_positive) != 0 else 0.0    
    r = f"{model_name} Precision: {precision}"
    print(r)
    t += '\n' + r

    # Evaluate recall
    true_positive = predictions.filter((predictions["Outcome"] == 1) & (predictions["prediction"] == 1)).count()
    false_negative = predictions.filter((predictions["Outcome"] == 1) & (predictions["prediction"] == 0)).count()
    recall = true_positive / (true_positive + false_negative) if (true_positive + false_negative) != 0 else 0.0
    r = f"{model_name} Recall: {recall}"
    print(r)
    t += '\n' + r

    return t


# Compare Different Models

# Evaluate Logistic Regression Model
a=evaluate_model(lr_model, test_data)

# Evaluate Random Forest Model
b=evaluate_model(rf_model, test_data)

# Evaluate Decision Tree Model
c=evaluate_model(dt_model, test_data)

with open(r"./diabetes_MLresult.txt", "w") as file: 
    file.write(a)
    file.write('\n ------------------\n')
    file.write(b)
    file.write('\n ------------------\n')
    file.write(c)

spark.stop()

