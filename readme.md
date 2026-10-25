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

--
scripts/submit.sh pipelines/pipeline4_load.py

docker exec -it retail-postgres psql -U postgres -d retail -c "SELECT count(*) FROM daily_sales_summary;"
docker exec -it retail-postgres psql -U postgres -d retail -c "SELECT * FROM top_products_by_category ORDER BY category, rank_in_category LIMIT 6;"
docker exec -it retail-postgres psql -U postgres -d retail -c "SELECT order_date, total_revenue FROM daily_sales_summary ORDER BY total_revenue DESC LIMIT 3;"



--
python scripts/generate_batch2.py