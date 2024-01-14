CREATE TABLE IF NOT EXISTS `$table_id` (
  id NUMERIC,
  starts TIMESTAMP,
  minutes NUMERIC,
  name STRING,
  workout_token STRING,
  workout_type_id NUMERIC,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  workout_summary_id NUMERIC,
  workout_summary_calories_accum NUMERIC,
  workout_summary_cadence_avg NUMERIC,
  workout_summary_distance_accum NUMERIC,
  workout_summary_duration_active_accum NUMERIC,
  workout_summary_duration_total_accum NUMERIC,
  workout_summary_power_avg NUMERIC,
  workout_summary_power_bike_np_last NUMERIC,
  workout_summary_power_bike_tss_last NUMERIC,
  workout_summary_speed_avg NUMERIC,
  workout_summary_work_accum NUMERIC,
  workout_summary_created_at TIMESTAMP,
  workout_summary_updated_at TIMESTAMP
);

MERGE `$table_id` P
USING `$stage_table_id` N
ON P.id = N.id
WHEN NOT MATCHED THEN
  INSERT 
  (
  id,
  starts,
  minutes,
  name,
  workout_token,
  workout_type_id,
  created_at,
  updated_at,
  workout_summary_id,
  workout_summary_calories_accum,
  workout_summary_cadence_avg,
  workout_summary_distance_accum,
  workout_summary_duration_active_accum,
  workout_summary_duration_total_accum,
  workout_summary_power_avg,
  workout_summary_power_bike_np_last,
  workout_summary_power_bike_tss_last,
  workout_summary_speed_avg,
  workout_summary_work_accum,
  workout_summary_created_at,
  workout_summary_updated_at
  ) 
  VALUES
  (
  id,
  starts,
  minutes,
  name,
  workout_token,
  workout_type_id,
  created_at,
  updated_at,
  workout_summary_id,
  workout_summary_calories_accum,
  workout_summary_cadence_avg,
  workout_summary_distance_accum,
  workout_summary_duration_active_accum,
  workout_summary_duration_total_accum,
  workout_summary_power_avg,
  workout_summary_power_bike_np_last,
  workout_summary_power_bike_tss_last,
  workout_summary_speed_avg,
  workout_summary_work_accum,
  workout_summary_created_at,
  workout_summary_updated_at
  )
