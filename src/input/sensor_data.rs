

struct SensorData{
    velocity : f64,
    speed_limit_kmh : f64,
    speed_limit_distance_m : u32, 
}

impl SensorData {
    fn new() -> Self{
        Self { velocity: 0.0, speed_limit_kmh: 10.0, speed_limit_distance_m: 10 }
    }

    fn speed_limit_to_m_s(&self) -> f64
    {
        self.speed_limit_kmh / 3.6
    }

    fn update_sensor_data(&mut self, velocity : f64, speed_limit_kmh : f64, speed_limit_distance : u32) 
    {
        self.speed_limit_distance_m = speed_limit_distance;
        self.speed_limit_kmh = speed_limit_kmh;
        self.speed_limit_distance_m = speed_limit_distance;
    }
}