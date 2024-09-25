SELECT w.weather AS conditions, 
       count(*) AS num_trips 
FROM Citibike.Test.Trips AS t  
LEFT OUTER JOIN Weather.Test.weather_data_view AS w    
ON date_trunc('hour', w.observation_time) = date_trunc('hour', t.starttime) 
WHERE conditions IS NOT NULL 
GROUP BY 1
ORDER BY 2 desc;