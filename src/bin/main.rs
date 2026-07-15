use swb_fault_tolerance::braking_curve::{BrakeInput, compute_braking_curve};

fn main() {
    let v0 = 80.0 / 3.6; // ≈ 22.222…
    let v_target = 30.0 / 3.6; // ≈ 8.333…
    let test: BrakeInput = BrakeInput {
        current_speed: (v0),
        target_speed: (v_target),
        available_distance: (1000.0),
    };
    let test_result = compute_braking_curve(test);
    let res = test_result.expect("FUCK");
    println!("{}", res.total_distance);
}
