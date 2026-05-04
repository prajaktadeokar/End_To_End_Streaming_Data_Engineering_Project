CREATE OR REFRESH STREAMING TABLE uber_catalog.silver_staging.silver_obt 
AS

SELECT 
  
         staging_rides.ride_id, staging_rides.confirmation_number, staging_rides.passenger_id, staging_rides.driver_id, staging_rides.vehicle_id, staging_rides.pickup_location_id, staging_rides.dropoff_location_id, staging_rides.vehicle_type_id, staging_rides.vehicle_make_id, staging_rides.payment_method_id, staging_rides.ride_status_id, staging_rides.pickup_city_id, staging_rides.dropoff_city_id, staging_rides.cancellation_reason_id, staging_rides.passenger_name, staging_rides.passenger_email, staging_rides.passenger_phone, staging_rides.driver_name, staging_rides.driver_rating, staging_rides.driver_phone, staging_rides.driver_license, staging_rides.vehicle_model, staging_rides.vehicle_color, staging_rides.license_plate, staging_rides.pickup_address, staging_rides.pickup_latitude, staging_rides.pickup_longitude, staging_rides.dropoff_address, staging_rides.dropoff_latitude, staging_rides.dropoff_longitude, staging_rides.distance_miles, staging_rides.duration_minutes, staging_rides.booking_timestamp, staging_rides.pickup_timestamp, staging_rides.dropoff_timestamp, staging_rides.base_fare, staging_rides.distance_fare, staging_rides.time_fare, staging_rides.surge_multiplier, staging_rides.subtotal, staging_rides.tip_amount, staging_rides.total_fare, staging_rides.rating
           
              ,
           
        
         map_vehicle_makes.vehicle_make
           
              ,
           
        
         map_vehicle_types.vehicle_type
           
              ,
           
        
         map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at
           
              ,
           
        
         map_cancellation_reasons.cancellation_reason
           
              ,
           
        
         map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth
           
              ,
           
        
         map_ride_statuses.ride_status
           
        
FROM 
     
        
          STREAM  (uber_catalog.silver_staging.staging_rides)
          WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES  staging_rides
         
      
        
              LEFT JOIN uber_catalog.bronze.map_vehicle_makes as map_vehicle_makes ON staging_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
         
      
        
              LEFT JOIN uber_catalog.bronze.map_vehicle_types as map_vehicle_types ON staging_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
         
      
        
              LEFT JOIN uber_catalog.bronze.map_cities as map_cities ON staging_rides.pickup_city_id = map_cities.city_id
         
      
        
              LEFT JOIN uber_catalog.bronze.map_cancellation_reasons as map_cancellation_reasons ON staging_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
         
      
        
              LEFT JOIN uber_catalog.bronze.map_payment_methods as map_payment_methods ON staging_rides.payment_method_id = map_payment_methods.payment_method_id
         
      
        
              LEFT JOIN uber_catalog.bronze.map_ride_statuses as map_ride_statuses ON staging_rides.ride_status_id = map_ride_statuses.ride_status_id
         
      

    
         
            
           
        