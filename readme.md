scripts/submit.sh pipelines/pipeline1_ingest.py

scripts/submit.sh scripts/verify_bronze.py

scripts/submit.sh pipelines/pipeline2_clean.py

scripts/submit.sh scripts/verify_silver.py


--exercise :
scripts/submit.sh scripts/exercises_step6.py


-- keep few minutes interval and run twice 
scripts/submit.sh pipelines/pipeline3_aggregate.py
scripts/submit.sh pipelines/pipeline3_aggregate.py


scripts/submit.sh scripts/verify_gold.py