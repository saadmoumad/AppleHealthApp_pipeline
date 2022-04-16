# AppleHealthApp_pipeline
### A data pipeline for fun

the idea of this project is to collect my fitness data from multiple devices and present it in the same dashboard, by automating a workflow of an airflow based pipeline.




### Notes:
- In order to implement this, you would need to configure Airflow, configure your own *mySQL* database and add in the appropriate connection variables for *Dropbox API*. All of this variables should be pre-defined in the 'Scripts/airflow_variables.json' file.
- You would also need to edit the **docker_compose.yaml** mainly to point the local dirs that contains our pipeline's custom scripts. (This repo uses the          The Apache **Airflow** community image with some changes, this configuration is for local development. Do not use it in a production deployment.)
      

      
