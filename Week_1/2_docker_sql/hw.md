# Question 1
`docker build --help`

# Question 2
```
docker run --entrypoint=bash python:3.9
pip list
```

# Question 3
```
SELECT count(distinct gt.index)
FROM green_taxi_data_2019 gt
where to_date(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-15'
```

# Question 4
```
SELECT to_date(lpep_pickup_datetime, 'YYYY-MM-DD'), max(gt.trip_distance) as max_dist
FROM green_taxi_data_2019 gt
group by to_date(lpep_pickup_datetime, 'YYYY-MM-DD')
order by max(gt.trip_distance) desc
limit 1
```

# Question 5
```
SELECT passenger_count, count(*)
FROM green_taxi_data_2019 gt
where to_date(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-01'
and passenger_count in (2, 3)
group by passenger_count
```

# Question 6
```
SELECT lk_do.zn as do_location, tip_amount
FROM green_taxi_data_2019 gt
left join taxi_lookup lk_do on lk_do.location_id=gt.do_location_id
left join taxi_lookup lk_pu on lk_pu.location_id=gt.pu_location_id
where lk_pu.zn = 'Astoria'
order by tip_amount desc
limit 1
```