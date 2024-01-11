## Diabetes Diagnosis Prediction
The goal of this project is to create a binary classifier to help for diabetes diagnosis.

## Instalation and running
The project was created to work on Windows 10.
To work, you need to install Spark according to the instructions
https://phoenixnap.com/kb/install-spark-on-windows-10.

After installation, you need to configure the environment variables.
To start the project, follow these steps:
   - launch the power shell terminal
   - go to the directory with the project
   - run the discarded file with the .\diabetes_ML_Spark_run.cmd command
Alternatively, instead of the terminal, you can simply double-click with the left mouse button in the Windows file explorer.

As a result, we will receive the file (diabetes_MLresult.txt) with the evaluation of three different machine learning models based on the dataset (diabetes.csv) according to the following іефтвфке steps:

### Steps
**1. Preprocessing**
- Create spark session
- load data
- clean missing values
- drop unnecessary columns

**2. Feature engineering**
from the useful columns create a vector using the VectorAssembler
Split between test and train 

**3. model training**
Train different models to test their performances using ml lib in spark

**4. Evaluation**
Compare the different models using accuracy, precision, recall.

**5. Nice to have**
Create a server sending the data and connect to it instead of reading the csv file.
