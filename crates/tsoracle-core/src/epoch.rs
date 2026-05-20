//! Leader epoch — opaque monotonic identifier chosen by the consensus driver.

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Epoch(pub u64);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zero() {
        assert_eq!(Epoch::default(), Epoch::ZERO);
    }

    #[test]
    fn ordering() {
        assert!(Epoch(1) < Epoch(2));
        assert!(Epoch::ZERO < Epoch(1));
    }
}
