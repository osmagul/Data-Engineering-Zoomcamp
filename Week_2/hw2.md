# 1
In etl_web_to_gcs.py just replace year, month, color arguments by 2020, 1, green. 

# 2
* Cron may be set in cmd: ``` prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n etl --cron "0 5 1 * *" ```
* Cron may be set in Prefect Deployments
* ```schedule: 
        cron: 0 5 1 * *
        timezone: Asia/Almaty
  ```
# 3 
* First upload Yellow taxi data for Feb. 2019 and March 2019 through etl_web_to_gcs to bucket
* ```prefect deployment build ./etl_gcs_to_bq.py:etl_gcs_to_bq -n "GCS to BQ Deployment" ```
* In generated yaml parameters: {"color": "yellow", "months": [2, 3], "year": 2019}
* ```prefect deployment apply etl_gcs_to_bq-deployment.yaml```
* ```prefect agent start -work-queue "default"```

# 4
* First create block (name: zoom-git)
* ```prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs --name zoom-git -q zoom-git -sb github/zoom-git```

* ```prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs --name zoom-git -q zoom-git -sb github/zoom-git --apply```

* ```prefect agent start -q zoom-git```

# 5
* Create Slack Webhook Notification
* Run commands from 4 task

# 6
Create secret block with 10 symboled password