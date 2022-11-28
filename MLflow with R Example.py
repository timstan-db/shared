# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/10/model-registry-new.png" height = 1200 width = 800>
# MAGIC _____
# MAGIC # MLflow Quick Start:  Training and Logging
# MAGIC 
# MAGIC In this tutorial we will:
# MAGIC 
# MAGIC - Install MLflow for R on a Databricks cluster
# MAGIC - Train a regression model on the `wine quality` dataset and log metrics, parameters and models
# MAGIC - View the results of training in the MLflow tracking UI
# MAGIC - Explore serving a model in batch or via REST
# MAGIC - Promote a model to the Model Registry 
# MAGIC - Do some Time Travel ;)
# MAGIC ___
# MAGIC 
# MAGIC ### Setup 
# MAGIC 
# MAGIC 1. Ensure you are using or create a cluster specifying Databricks Runtime version 5.0 or above
# MAGIC 2. Attach this notebook to the cluster
# MAGIC 
# MAGIC ### Installing ML Flow

# COMMAND ----------

# MAGIC %r
# MAGIC ## Install ML FLow from CRAN, and carrier as well
# MAGIC install.packages('mlflow')
# MAGIC install.packages('carrier')
# MAGIC 
# MAGIC ## Load the library and others we need for the notebook
# MAGIC library(mlflow)
# MAGIC library(httr)
# MAGIC library(SparkR)
# MAGIC library(glmnet)
# MAGIC library(carrier)
# MAGIC 
# MAGIC ## Complete the installation
# MAGIC # install_mlflow()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Organize MLflow Runs into Experiments
# MAGIC 
# MAGIC As you start using your MLflow server for more tasks, you may want to separate them out. MLflow allows you to create experiments to organize your runs. To report your run to a specific experiment, specify the path to your experiment in your Workspace using the `mlflow_set_experiment()` function.  By default this will be the path to the Databricks Notebook you are working in.

# COMMAND ----------

# MAGIC %r
# MAGIC # mlflow_set_experiment("/Users/tim.stanton@databricks.com/R_on_Databricks/Wine Quality Estimator")

# COMMAND ----------

# MAGIC %md
# MAGIC  #### Load Wine Quality Data 

# COMMAND ----------

# MAGIC %r
# MAGIC # Read the wine-quality csv file
# MAGIC wine_quality <- read.csv("https://raw.githubusercontent.com/mlflow/mlflow-example/master/wine-quality.csv")
# MAGIC 
# MAGIC head(wine_quality)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Function to Train with Input Parameters

# COMMAND ----------

# MAGIC %r
# MAGIC ## Create a function to train based on different parameters
# MAGIC train_wine_quality <- function(data, alpha, lambda) {
# MAGIC 
# MAGIC # Split the data into training and test sets. (0.75, 0.25) split.
# MAGIC sampled <- base::sample(1:nrow(data), 0.75 * nrow(data))
# MAGIC train <- data[sampled, ]
# MAGIC test <- data[-sampled, ]
# MAGIC 
# MAGIC # The predicted column is "quality" which is a scalar from [3, 9]
# MAGIC train_x <- as.matrix(train[, !(names(train) == "quality")])
# MAGIC test_x <- as.matrix(test[, !(names(train) == "quality")])
# MAGIC train_y <- train[, "quality"]
# MAGIC test_y <- test[, "quality"]
# MAGIC 
# MAGIC ## Define the parameters used in each MLflow run
# MAGIC alpha <- mlflow_param("alpha", alpha, "numeric")
# MAGIC lambda <- mlflow_param("lambda", lambda, "numeric")
# MAGIC 
# MAGIC with(mlflow_start_run(), {
# MAGIC     model <- glmnet(train_x, train_y, alpha = alpha, lambda = lambda, family= "gaussian", standardize = FALSE)
# MAGIC     l1se <- cv.glmnet(train_x, train_y, alpha = alpha)$lambda.1se
# MAGIC     predictor <- carrier::crate(~ glmnet::predict.glmnet(!!model, as.matrix(.x)), !!model, s = l1se)
# MAGIC   
# MAGIC     predicted <- predictor(test_x)
# MAGIC 
# MAGIC     rmse <- sqrt(mean((predicted - test_y) ^ 2))
# MAGIC     mae <- mean(abs(predicted - test_y))
# MAGIC     r2 <- as.numeric(cor(predicted, test_y) ^ 2)
# MAGIC 
# MAGIC     message("Elasticnet model (alpha=", alpha, ", lambda=", lambda, "):")
# MAGIC     message("  RMSE: ", rmse)
# MAGIC     message("  MAE: ", mae)
# MAGIC     message("  R2: ", mean(r2, na.rm = TRUE))
# MAGIC 
# MAGIC     ## Log the parameters associated with this run
# MAGIC     mlflow_log_param("alpha", alpha)
# MAGIC     mlflow_log_param("lambda", lambda)
# MAGIC   
# MAGIC     ## Log metrics we define from this run
# MAGIC     mlflow_log_metric("rmse", rmse)
# MAGIC     mlflow_log_metric("r2", mean(r2, na.rm = TRUE))
# MAGIC     mlflow_log_metric("mae", mae)
# MAGIC   
# MAGIC     # Save plot to disk
# MAGIC     png(filename = "ElasticNet-CrossValidation.png")
# MAGIC     plot(cv.glmnet(train_x, train_y, alpha = alpha), label = TRUE)
# MAGIC     dev.off()
# MAGIC   
# MAGIC     ## Log that plot as an artifact
# MAGIC     mlflow_log_artifact("ElasticNet-CrossValidation.png")
# MAGIC 
# MAGIC     mlflow_log_model(predictor, "model")
# MAGIC   
# MAGIC })
# MAGIC   }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training Runs with Different Hyperparameters
# MAGIC 
# MAGIC You could, of course, train with different data sets here as well.

# COMMAND ----------

# MAGIC %r
# MAGIC set.seed(98118)
# MAGIC 
# MAGIC ## Run 1
# MAGIC train_wine_quality(data = wine_quality, alpha = 0.03, lambda = 0.98)
# MAGIC 
# MAGIC ## Run 2
# MAGIC train_wine_quality(data = wine_quality, alpha = 0.14, lambda = 0.4)
# MAGIC 
# MAGIC ## Run 3
# MAGIC train_wine_quality(data = wine_quality, alpha = 0.20, lambda = 0.88)

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC #### Promote to MLflow Model Registry
# MAGIC 
# MAGIC After experimenting and converging on a best model, we can push this model to the MLflow Model Registry.  This provides a host of benefits: <br><br>
# MAGIC 
# MAGIC * **One collaborative hub for model discovery and knowledge sharing**
# MAGIC * **Model lifecycle management tools to improve reliability and robustness of the deployment process**
# MAGIC * **Visibility and governance features for different deployment stages of each model**
# MAGIC 
# MAGIC Let's explore how to interact with the registry programmatically.  Please note that as of May 2020 we will need to use the Python API for this section, but we'll be registering and working with R models.

# COMMAND ----------

import mlflow

# Register the model with the run URI and unique name
model_uri = "runs:/f2d1da2bfe5942a4889b891019e5d70d/model"

model_details = mlflow.register_model(model_uri=model_uri, name="wine_quality_estimator")

# COMMAND ----------

# If we wanted to add another version to the registered model
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
result = client.create_model_version(
    name="wine_quality_estimator",
    source="dbfs:/databricks/mlflow-tracking/3183001650087415/d76bc845af4847b6988334f3d0dc1155/artifacts/model",
    run_id="d76bc845af4847b6988334f3d0dc1155"
)

# COMMAND ----------

# Transition stage to production
client.transition_model_version_stage(
    name="wine_quality_estimator",
    version=4,
    stage="Production"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Model Inference with Registered Models

# COMMAND ----------

# MAGIC %r
# MAGIC # Reload the production model into memory
# MAGIC # Docs: https://mlflow.org/docs/latest/R-api.html#details-1
# MAGIC prod_model <- mlflow_load_model(model_uri = "models:/wine_quality_estimator/production")
# MAGIC 
# MAGIC ## Generate prediction on 5 rows of data 
# MAGIC predictions <- data.frame(mlflow_predict(prod_model, data = wine_quality[1:5, !(names(wine_quality) == "quality")]))
# MAGIC                           
# MAGIC names(predictions) <- "wine_quality_pred"
# MAGIC 
# MAGIC ## Take a look
# MAGIC display(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the MLflow UI
# MAGIC Visit your tracking server by opening up the experiment in your workspace.
# MAGIC 
# MAGIC Inside the UI, you can:
# MAGIC * View your experiments and runs
# MAGIC * Review the parameters and metrics on each run
# MAGIC * Click each run for a detailed view to see the the model, images, and other artifacts produced.
# MAGIC 
# MAGIC _________
# MAGIC 
# MAGIC ### Model Serving via Batch Process
# MAGIC 
# MAGIC Here we demonstrate using the MLflow API to load a model from the MLflow server that was produced by a given run.  This requires specifying the `run_id`.
# MAGIC 
# MAGIC Once loaded, it is a `glmnet` model object like any other and we can reuse it as such.

# COMMAND ----------

# MAGIC %r
# MAGIC ## Load the model
# MAGIC best_model <- mlflow_load_model(model_uri = "dbfs:/databricks/mlflow-tracking/3183001650087415/d76bc845af4847b6988334f3d0dc1155/artifacts/model")
# MAGIC 
# MAGIC ## Generate prediction on 5 rows of data 
# MAGIC predictions <- data.frame(mlflow_predict(best_model, data = wine_quality[1:5, !(names(wine_quality) == "quality")]))
# MAGIC                           
# MAGIC names(predictions) <- "wine_quality_pred"
# MAGIC 
# MAGIC # ## Take a look
# MAGIC display(predictions)
