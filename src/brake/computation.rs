use crate::brake::braking_curve::{
    compute_braking_curve, BrakeError, BrakeInput, BrakeResult,
};
use crate::framework::traits::Computation;

/// Adapter: verpackt `compute_braking_curve` als Framework-`Computation`.
pub struct BrakeComputation;

impl Computation for BrakeComputation {
    type Input = BrakeInput;
    type Payload = BrakeResult;
    type Error = BrakeError;

    fn compute(&mut self, input: BrakeInput) -> Result<BrakeResult, BrakeError> {
        compute_braking_curve(input)
    }
}